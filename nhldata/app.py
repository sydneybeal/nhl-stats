'''
	This is the NHL crawler.  

Scattered throughout are TODO tips on what to look for.

Assume this job isn't expanding in scope, but pretend it will be pushed into production to run 
automomously.  So feel free to add anywhere (not hinted, this is where we see your though process..)
    * error handling where you see things going wrong.  
    * messaging for monitoring or troubleshooting
    * anything else you think is necessary to have for restful nights
'''
import logging
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
import boto3
import requests
import pandas as pd
from botocore.config import Config
from dateutil.parser import parse as dateparse
import json
import time

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger(__name__)

class NHLApi:
    SCHEMA_HOST = "https://statsapi.web.nhl.com/"
    VERSION_PREFIX = "api/v1"

    def __init__(self, base=None):
        self.base = base if base else f"{self.SCHEMA_HOST}/{self.VERSION_PREFIX}"


    def schedule(self, start_date: datetime, end_date: datetime) -> dict:
        ''' 
        returns a dict tree structure that is like
            "dates": [ 
                {
                    " #.. meta info, one for each requested date ",
                    "games": [
                        { #.. game info },
                        ...
                    ]
                },
                ...
            ]
        '''
        LOG.info(f"getting schedule from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        return self._get(self._url('schedule'), {'startDate': start_date.strftime('%Y-%m-%d'), 'endDate': end_date.strftime('%Y-%m-%d')})

    def boxscore(self, game_id):
        '''
        returns a dict tree structure that is like
           "teams": {
                "home": {
                    " #.. other meta ",
                    "players": {
                        $player_id: {
                            "person": {
                                "id": $int,
                                "fullName": $string,
                                #-- other info
                                "currentTeam": {
                                    "name": $string,
                                    #-- other info
                                },
                                "stats": {
                                    "skaterStats": {
                                        "assists": $int,
                                        "goals": $int,
                                        #-- other status
                                    }
                                    #-- ignore "goalieStats"
                                }
                            }
                        },
                        #...
                    }
                },
                "away": {
                    #... same as "home" 
                }
            }

            See tests/resources/boxscore.json for a real example response
        '''
        url = self._url(f'game/{game_id}/boxscore')
        return self._get(url)

    def _get(self, url, params=None):
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def _url(self, path):
        return f'{self.base}/{path}'

@dataclass
class StorageKey:
    # TODO what properties are needed to partition?
    tablenm: str
    gameid: str
    gamedt: str

    def key(self):
        ''' renders the s3 key for the given set of properties '''
        # TODO use the properties to return the s3 key
        return f'{self.tablenm}/{self.gamedt}/{self.gameid}.csv'

class Storage():
    def __init__(self, dest_bucket, s3_client):
        self._s3_client = s3_client
        self.bucket = dest_bucket

    def store_game(self, key: StorageKey, game_data) -> bool:
        LOG.info(f"Storing {type(game_data)} to {key.key()}")
        self._s3_client.put_object(Bucket=self.bucket, Key=key.key(), Body=game_data)
        return True

class Crawler():
    def __init__(self, api: NHLApi, storage: Storage):
        self.api = api
        self.storage = storage

    def get_players(self, players, side):
        """
            returns list of dict of the values that we want for each player
        """
        player_list = []
        for k in players:
            player = players[k]
            player_id = k[2:]
            if player.get('person').get('currentTeam'):
                currentTeam = player.get('person').get('currentTeam').get('name')
            else:
                currentTeam = None
            if player.get('person').get('fullName'):
                fullName = player.get('person').get('fullName')
            else:
                fullName = None
            # ignore goalies (players with "goalieStats")
            if player.get('stats') and "goalieStats" not in player.get('stats'):
                skaterStats_assists = player.get('stats').get('skaterStats').get('assists')
                stats_skaterStats_goals = player.get('stats').get('skaterStats').get('goals')
            else:
                skaterStats_assists = None
                stats_skaterStats_goals = None
            player_to_add = {
                "player_person_id": player_id,
                "player_person_currentTeam_name": currentTeam,
                "player_person_fullName": fullName,
                "player_stats_skaterStats_assists": skaterStats_assists if skaterStats_assists is not None else 0,
                "player_stats_skaterStats_goals": stats_skaterStats_goals if stats_skaterStats_goals is not None else 0,
                "side": side
            }
            player_list.append(player_to_add)
        return player_list

    def crawl(self, startDate: datetime, endDate: datetime) -> None:
        # error handling
        try:
            data = self.api.schedule(startDate, endDate)
            data_df = pd.json_normalize(data)
            if len(data_df.dates[0]) == 0:
                LOG.info("There were no NHL games for this date range")
                error_entry = [{
                    "player_person_id": 1,
                    "player_person_currentTeam_name": f"error getting players for {endDate.strftime('%Y-%m-%d')}",
                    "player_person_fullName": "error",
                    "player_stats_skaterStats_assists": 0,
                    "player_stats_skaterStats_goals": 0,
                    "side": "error"
                }]
                try:
                    LOG.info(f"saving {error_entry} to minio for date {endDate.strftime('%Y-%m-%d')}")
                    self.storage.store_game(
                        StorageKey(
                            tablenm="player_game_stats", gameid=1, gamedt=endDate.strftime('%Y-%m-%d')
                        ),
                        pd.DataFrame(error_entry).to_csv(index=False)
                    )
                except Exception as e:
                    LOG.error(f"Failed to save error message to MinIO, error: {e}")
                    raise Exception
                exit(0)
            dates_df = pd.json_normalize(data_df.dates[0])
        except Exception as e:
            LOG.error(f"Failed to get data from API, error: {e}")
            raise Exception
        # get games for dates
        for date in dates_df.itertuples():
            for game in date.games:
                players = []
                game_date = date.date
                boxscore_data = self.api.boxscore(game.get("gamePk")).get('teams')
                for side in ['home', 'away']:
                    players += self.get_players(boxscore_data.get(side).get('players'), side)
                try:
                    self.storage.store_game(
                        StorageKey(
                            tablenm="player_game_stats", gameid=game.get("gamePk"), gamedt=game_date
                        ),
                        pd.DataFrame(players).to_csv(index=False)
                    )
                except Exception as e:
                    LOG.error(f"Failed to save data to MinIO, error: {e}")
                    raise Exception


def main():
    import os
    import argparse
    parser = argparse.ArgumentParser(description='NHL Stats crawler')
    # TODO pass start and end dates to makefile, dockercompose, dockerfile - maybe dev/prod flags
    args = parser.parse_args()

    retries = 5
    wait_seconds = 60 * 5
    # wait_seconds = 5

    dest_bucket = os.environ.get('DEST_BUCKET', 'output')
    startDate = datetime(2021, 12, 10) # TODO get from args
    endDate = datetime(2021, 12, 15)  # TODO get from args

    for attempt in range(retries):
        try:
            if attempt == 0:
                raise Exception("First attempt has been manually failed to demonstrate retries")
            api = NHLApi()
            s3client = boto3.client(
                's3',
                config=Config(signature_version='s3v4'),
                endpoint_url=os.environ.get('S3_ENDPOINT_URL')
            )
            storage = Storage(dest_bucket, s3client)
            crawler = Crawler(api, storage)
            crawler.crawl(startDate, endDate)
        except Exception as e:
            LOG.warning(
                f"Attempt #{attempt+1} has failed, retrying after {wait_seconds}s until {retries} attempts. ERROR: {e}"
            )
            time.sleep(wait_seconds)
        else:
            break
    else:
        LOG.error(f"All attempts to run have failed, notifying engineering team")
        # send to slack webhook
        # kill container


if __name__ == '__main__':
    main()
