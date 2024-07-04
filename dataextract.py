import requests
import csv
import json

def fetch_data(page_size, page_number):
    #base_url = "https://api.apps1.nsw.gov.au/eplanning/data/v0/OnlineDA"
    base_url = "https://api.apps1.nsw.gov.au/eplanning/data/v0/OnlineCDC"
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
                print("Application: ")
                print(application)
        #for location in application.get("Location", []):
        #    for lot in location.get("Lot", []):
                first_location = application.get("Location", [{}])[0]
                first_lot = first_location.get("Lot", [{}])[0]
                extracted.append({
                    "PlanningPortalApplicationNumber": application.get("PlanningPortalApplicationNumber"),
                    "CostOfDevelopment": application.get("CostOfDevelopment"),
                    "ApplicationStatus": application.get("ApplicationStatus"),
                    "ApplicationType": application.get("ApplicationType"),
                    "CouncilName": application.get("Council", {}).get("CouncilName"),
                    "DevelopmentType": ", ".join([dt.get("DevelopmentType") for dt in application.get("DevelopmentType", [])]),
                    "FullAddress": first_location.get("FullAddress"),
                    "StreetNumber1": first_location.get("StreetNumber1"),
                    "StreetType": first_location.get("StreetType"),
                    "StreetName": first_location.get("StreetName"),
                    "Suburb": first_location.get("Suburb"),
                    "PostCode": first_location.get("Postcode"),
                    "State": first_location.get("State"),
                    "PlanLabel": first_lot.get("PlanLabel"),
                    "ModificationApplicationNumber": application.get("ModificationApplicationNumber"),
                    "LodgementDate": application.get("LodgementDate"),
                    "DeterminationDate": application.get("DeterminationDate"),
                })                
    print("extracted: ")
    print(extracted)            
    return extracted

def write_to_csv(data, filename="output.csv"):
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=["PlanningPortalApplicationNumber", "CostOfDevelopment",
                                                "ApplicationStatus", "ApplicationType", "CouncilName", "DevelopmentType", 
                                                "FullAddress", "StreetNumber1", "StreetType",
                                                "StreetName", "Suburb", "PostCode", "State", "PlanLabel",
                                                "ModificationApplicationNumber", "LodgementDate", "DeterminationDate"])
        writer.writeheader()
        for row in data:
            writer.writerow(row)

def main():
    page_size = 2
    #page_number = 148024
    page_number = 69085
    all_data = []

    while True:
        data = fetch_data(page_size, page_number)
        if not data:
            break
        extracted_data = extract_data(data)
        all_data.extend(extracted_data)
        if page_number >= data.get("TotalPages", 0):
            break
        page_number += 1

    write_to_csv(all_data)

if __name__ == "__main__":
    main()