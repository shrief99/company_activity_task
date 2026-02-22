def fetch_api_data(start_date, end_date, max_retries=3):
    current_date = start_date
    while current_date <= end_date:
        api_url = f"URL"
        attempt = 0
        success = False
        
        while attempt < max_retries and not success:
            attempt += 1
            response = call_api(api_url) 
            
            if response.status_code == 200:
                data = response.json()
                
                if not data or 'company_id' not in data[0]:
                    log_error(f"Invalid or empty data for {current_date}")
                    break
                   
                try:
                    insert_into_staging("stg_api", data)
                    success = True
                except Exception as e:
                    log_error(f"DB insert failed for {current_date}: {e}")
            
            else:
                log_error(f"API call failed for {current_date}: {response.status_code}")
        
        if not success:
            log_error(f"Failed to ingest data for {current_date} after {max_retries} attempts")
        
        current_date += timedelta(days=1)
    print("API ingestion completed.")
