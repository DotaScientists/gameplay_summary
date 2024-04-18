from gameplay_summary.settings import Settings, Constants, PROJECT_ROOT
from gameplay_summary.services.dataset_creator import DatasetCreator
from pathlib import Path

def main():
    settings = Settings()
    constants = Constants()
    dataset_creator = DatasetCreator(settings, constants)
    dataset_creator.create_dataset()

if __name__ == "__main__":
    import os
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(PROJECT_ROOT / "keys/key.json")
    main()