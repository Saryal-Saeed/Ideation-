import os
import subprocess
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler()
    ]
)

def run_script(script_name):
    """Run a Python script and log its output"""
    try:
        logging.info(f"Starting {script_name}...")
        result = subprocess.run(['python', script_name], 
                              capture_output=True, 
                              text=True,
                              check=True)
        logging.info(f"Completed {script_name}")
        if result.stdout:
            logging.info(f"Output from {script_name}:\n{result.stdout}")
        if result.stderr:
            logging.warning(f"Warnings from {script_name}:\n{result.stderr}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error running {script_name}: {str(e)}")
        logging.error(f"Error output: {e.stderr}")
        raise

def main():
    try:
        # Step 1: Run Techcrunch.py to scrape articles
        run_script('Techcrunch.py')
        
        # Step 2: Run extracting_insights.py to process articles
        run_script('extracting_insights.py')
        
        # Step 3: Run CleanedCSV.py to clean the data
        run_script('CleanedCSV.py')
        
        # Step 4: Run Separate_CSVS.py to create separate CSV files
        run_script('Separate_CSVS.py')
        
        # Step 5: Update Google Sheets
        run_script('update_google_sheets.py')
        
        logging.info("Pipeline completed successfully!")
        
    except Exception as e:
        logging.error(f"Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    main() 