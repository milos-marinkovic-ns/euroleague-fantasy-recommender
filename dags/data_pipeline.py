import yaml
import requests
import json
import os
from time import sleep

class EuroleagueDataConnector:

    api_config_path: str = r"dags/configs/api_configs.yaml"


    def __init__(self):
        
        with open(self.api_config_path, "r") as file:
            config = yaml.safe_load(file)
        
        self.box_score_base_url: str = config["euroleague_api_config"]["box_score_base_url"]
        self.seasons_games: list[list[str]] = config["euroleague_api_config"]["box_score_queries"]

    def save_to_json(self, data, filename):
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "w") as file:
            json.dump(data, file, indent=4)


    def fetch_old_box_score_data(self):
        for season_code, max_game_id in self.seasons_games:
            for game_id in range(1, int(max_game_id) + 1):
                url = f"{self.box_score_base_url}?gamecode={game_id}&seasoncode={season_code}"
                res = requests.get(url)

                if res.status_code == 200:
                    try:
                        self.save_to_json(res.json(), f"data/boxscore_json/{season_code}/{game_id}.json")
                        print(f"Fetched data for season: {season_code} game ID: {game_id}")
                    except:
                        continue
                else:
                    print(f"Failed to fetch data for season: {season_code} game ID: {game_id} - Status: {res.status_code}")

                sleep(0.01)




edc: EuroleagueDataConnector = EuroleagueDataConnector()


print(edc.box_score_base_url)
print(edc.seasons_games)

edc.fetch_old_box_score_data()