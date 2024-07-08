import requests
import csv
import json
import pandas as pd

def fetch_data(page_size, page_number):
    base_url = "https://api.apps1.nsw.gov.au/eplanning/data/v0/OnlineDA"
    #base_url = "https://api.apps1.nsw.gov.au/eplanning/data/v0/OnlineCDC"
    filters = json.dumps({"filters": {}})

    headers = {
        "PageSize": str(page_size),
        "PageNumber": str(page_number),
        "filters": filters
    }
    response = requests.get(base_url, headers=headers)
    if response.status_code == 200:
        #print(response.json())
        return response.json()
    else:
        print(f"Failed to fetch data: {response.status_code}")
        return None

def extract_data(data):
    #print("Data: ")
    #print(data)
    extracted = []

    for application in data.get("Application", []):
                #print("Application: ")
                #print(application)
        #for location in application.get("Location", []):
        #    for lot in location.get("Lot", []):
                first_location = application.get("Location", [{}])[0]
                first_lot = first_location.get("Lot", [{}])[0]
                locationarr =[]
                for location in application.get("Location", []):
                    locationarr.append({
                        "FullAddress": location.get("FullAddress"),
                        "StreetNumber1": location.get("StreetNumber1"),
                        "StreetType": location.get("StreetType"),
                        "StreetName": location.get("StreetName"),
                        "Suburb": location.get("Suburb"),
                        "PostCode": location.get("Postcode"),
                        "State": location.get("State"),
                        "PlanLabel": ", ".join([dt.get("PlanLabel") for dt in location.get("Lot", []) if dt.get("PlanLabel")]),
                    })

                extracted.append({
                    "PlanningPortalApplicationNumber": application.get("PlanningPortalApplicationNumber"),
                    "LodgementDate": application.get("LodgementDate"),
                    "DeterminationDate": application.get("DeterminationDate"),
                    "CostOfDevelopment": application.get("CostOfDevelopment"),
                    "ApplicationType": application.get("ApplicationType"),
                    "ApplicationStatus": application.get("ApplicationStatus"),                   
                    "CouncilName": application.get("Council", {}).get("CouncilName"),
                    "ModificationApplicationNumber": application.get("ModificationApplicationNumber"),
                    "DevelopmentType": ", ".join([dt.get("DevelopmentType") for dt in application.get("DevelopmentType", [])]),
                    #"FullAddress": first_location.get("FullAddress"),
                    #"StreetNumber1": first_location.get("StreetNumber1"),
                    #"StreetType": first_location.get("StreetType"),
                    #"StreetName": first_location.get("StreetName"),
                    #"Suburb": first_location.get("Suburb"),
                    #"PostCode": first_location.get("Postcode"),
                    #"State": first_location.get("State"),
                    #"PlanLabel": ", ".join([dt.get("PlanLabel") for dt in first_location.get("Lot", [])]),
                    "location": locationarr
                })                
    #print("extracted: ")
    #print(extracted)            
    return extracted

def write_to_csv(data, filename="output.csv"):
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=["PlanningPortalApplicationNumber","LodgementDate", 
                                                "DeterminationDate","CostOfDevelopment",
                                                "ApplicationType","ApplicationStatus",  "CouncilName", "ModificationApplicationNumber",
                                                "DevelopmentType","location" ])
        #"FullAddress", "StreetNumber1", "StreetType","StreetName", "Suburb", "PostCode", "State", "PlanLabel"
        writer.writeheader()
        for row in data:
            writer.writerow(row)

def split_Into_Columns():
    df = pd.read_csv('output.csv')
    split_columns = df['DevelopmentType'].str.split(',', expand=True)
    
    # Naming the new columns based on the split
    for i in range(split_columns.shape[1]):
        df[f'DevelopmentType_{i+1}'] = split_columns[i]
    
    split_columns = df['location'].str.split(',', expand=True)
    # Naming the new columns based on the split
    for i in range(split_columns.shape[1]):
        df[f'location_{i+1}'] = split_columns[i]
    df.to_csv('output.csv', index=False)
    #print(df)

def main():
    page_size = 1000
    page_number = 1
    #page_number = 69085
    all_data = []


    while True:
        data = fetch_data(page_size, page_number)
        if not data:
            break
        try:
            extracted_data = extract_data(data)
            all_data.extend(extracted_data)
        except Exception as e:
            print(f"Failed to extract data: {e}")
        
        if page_number >= data.get("TotalPages", 0):
                break
        page_number += 1
        print(page_number)
    try:
        write_to_csv(all_data)
        split_Into_Columns()
    except Exception as e:
        print(f"Failed to write data to CSV: {e}")

if __name__ == "__main__":
    main()