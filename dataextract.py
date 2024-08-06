from functools import reduce
import os
import re
import requests
import csv
import json
import pandas as pd

def fetch_data(base_url,page_size, page_number):

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

def write_to_csv(data, filename):
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

def filter_data(inputfilename,outputfilename,search_patterns_development_type,search_patterns_location):
    df = pd.read_csv(inputfilename)
    #print(df.head())

    # Apply the filter
    #filtered_df = df[df['DevelopmentType'].str.contains(search_patterns_development_type, case=False, na=False)]
    # Generate dynamic filter
    conditions = [df['DevelopmentType'].str.contains(pattern, case=False, na=False) for pattern in search_patterns_development_type]
    combined_condition = reduce(lambda x, y: x | y, conditions)
    # Ensure the boolean Series has the same index as the DataFrame
    combined_condition = combined_condition.reindex(df.index)
    filtered_df2 = df[combined_condition]
    #print(filtered_df)
    filtered_df2.to_csv('filterdevdata.csv', index=False)

    if(search_patterns_location == []):
        filtered_df = filtered_df2
    else:
        df2 = pd.read_csv('filterdevdata.csv')
        combined_pattern = '|'.join(search_patterns_location)
        filtered_df = df2.query('location.str.contains(@combined_pattern, case=False, na=False)')    
        #print(filtered_df)
        filtered_df.to_csv('filterlocdata.csv', index=False)

    # Assuming 'location' column contains structured text like "FullAddress: [address], PostCode: [code], State: [state], PlanLabel: [label]"
    # Define regex pattern to extract values
    # Adjust the regex based on the actual structure of your 'location' column
    FullAddresspattern = r'FullAddress(?P<FullAddress>.+?)(?=, \'StreetNumber1\')'
    #FullAddresspattern = r'FullAddress(?P<FullAddress>[^,]+)'
    PlanLabelpattern = r'PlanLabel(?P<PlanLabel>[^,]+)'
    
    # Extract values into new columns
    df_extracted_FullAddress = filtered_df['location'].str.extract(FullAddresspattern)
    # Assuming df_extracted_FullAddress is your DataFrame and 'FullAddress' is the column
    df_extracted_FullAddress['FullAddress'] = df_extracted_FullAddress['FullAddress'].str.replace(r'[^a-zA-Z0-9\s]', '', regex=True)
    #print(df_extracted_FullAddress)
    df_extracted_PlanLabel = filtered_df['location'].str.extract(PlanLabelpattern)
    df_extracted_PlanLabel['PlanLabel'] = df_extracted_PlanLabel['PlanLabel'].str.replace(r'[^a-zA-Z0-9\s]', '', regex=True)
    
    #print(df_extracted_PlanLabel)

    # Display the filtered DataFrame
    # Merge the extracted data with the original DataFrame
    filtered_df.drop(columns=['location'], inplace=True)
    merged_df = pd.concat([filtered_df, df_extracted_FullAddress,df_extracted_PlanLabel], axis=1)
    merged_df.to_csv(outputfilename, index=False) 

def merge_data(filter_output_file_DA,filter_output_file_CDC,merge_output_file):
     # Read the CSV files
    df_DA = pd.read_csv(filter_output_file_DA)
    df_CDC = pd.read_csv(filter_output_file_CDC)

    # Merge the DataFrames
    # Assuming you want to concatenate them vertically (one after the other)
    # If you need a different kind of merge, adjust accordingly
    merged_df = pd.concat([df_DA, df_CDC], ignore_index=True)

    # Save the merged DataFrame to a new CSV file
    merged_df.to_csv(merge_output_file, index=False)

def process_data(base_url, page_size, page_number,source_output_file):
    all_data = []
    while True:
        data = fetch_data(base_url,page_size, page_number)
        #print(page_number)
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
        write_to_csv(all_data, source_output_file)
        #split_Into_Columns()
    except Exception as e:
        print(f"Failed to write data to CSV: {e}")

def read_input_txt_data(inputfilename):
     # Read the contents of the file
    with open('inputs.txt', 'r') as file:
        content = file.read()

    # Extract the values using regular expressions
    development_type_match = re.search(r'search_patterns_development_type\s*=\s*(.*)', content)
    location_match = re.search(r'search_patterns_location\s*=\s*(.*)', content)

    # Convert the extracted strings into lists
    if development_type_match and location_match:
        search_patterns_development_type = eval(development_type_match.group(1))
        search_patterns_location = eval(location_match.group(1))

        print("Development Types:", search_patterns_development_type)
        print("Locations:", search_patterns_location)
    else:
        print("Patterns not found in the file.")
    return search_patterns_development_type,search_patterns_location

def main():
    page_size = 10000
    page_number = 1
    #page_number = 69085
    all_data = []
    base_url_onlineDA = "https://api.apps1.nsw.gov.au/eplanning/data/v0/OnlineDA"
    base_url_onlineCDC = "https://api.apps1.nsw.gov.au/eplanning/data/v0/OnlineCDC"
    output_file_DA = 'outputDA.csv'
    output_file_CDC = 'outputCDC.csv'
    filter_output_file_DA = 'filteroutputDA.csv'
    filter_output_file_CDC = 'filteroutputCDC.csv'
    merge_output_file = 'childcenters.csv'
    #search_pattern = 'child'
    search_patterns_development_type, search_patterns_location= read_input_txt_data("inputs.txt")
    #search_patterns_development_type = ["childcare", "child care", "daycare", "day care", "early learning", "preschool","Others"]  # Add more patterns as needed
    #search_patterns_location = ["2207","2170","2142"]  # Add more patterns as needed

    #data extracted for both DA and CDC
    process_data(base_url_onlineDA, page_size, page_number,output_file_DA)
    filter_data(output_file_DA,filter_output_file_DA,search_patterns_development_type,search_patterns_location)

    process_data(base_url_onlineCDC, page_size, page_number,output_file_CDC)
    filter_data(output_file_CDC,filter_output_file_CDC,search_patterns_development_type,search_patterns_location)

    #Merge both the data DA and CDC files
    merge_data(filter_output_file_DA,filter_output_file_CDC,merge_output_file)
    
    #Remove the addtional file
    os.remove(filter_output_file_DA)
    os.remove(filter_output_file_CDC)

if __name__ == "__main__":
    main()