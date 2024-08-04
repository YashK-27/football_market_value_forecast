import pandas as pd
from pathlib import Path

class FootballDataLoader:
    DATE_COLUMNS = {
        'players': ['date_of_birth'],
        'appearances': ['date'],
        'game_events': ['date'],
        'player_valuations': ['date']
    }

    def __init__(self, base_path='football-analytics/data'):
        self.base_path = Path(base_path)
        self.datasets = {}
        self.load_datasets()

    def load_datasets(self):
        for file in self.base_path.glob('*.*'):
            name = file.stem
            if file.suffix == '.csv':
                df = pd.read_csv(file)
            elif file.suffix == '.parquet':
                df = pd.read_parquet(file)
            else:
                print(f"Skipping unsupported file format: {file.name}")
                continue
            
            if name in self.DATE_COLUMNS:
                for col in self.DATE_COLUMNS[name]:
                    df[col] = pd.to_datetime(df[col])
            
            self.datasets[name] = df
            print(f"Loaded: {file.name}")

    def __getattr__(self, name):
        if name in self.datasets:
            return self.datasets[name]
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")

    def get_dataset(self, name):
        return self.datasets.get(name)

    def list_datasets(self):
        return list(self.datasets.keys())

    def describe_datasets(self):
        for name, df in self.datasets.items():
            print(f"\nDataset: {name}")
            print(f"Shape: {df.shape}")
            print("Columns:")
            for col in df.columns:
                print(f"- {col}: {df[col].dtype}")
            print("\n" + "-"*50)
