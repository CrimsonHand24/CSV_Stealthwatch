import pandas as pd
import numpy as np
import ipaddress
import colorama
from colorama import Fore
colorama.init(autoreset=True)

file_path = '/home/joshua/Desktop/StealthWatch/'
flow_analysis_file = 'flowAnalysis-2023.05.30-07.04.54.csv'
file_path_Geo = '/home/joshua/Desktop/StealthWatch/'
GEO_Light_Country = 'GeoLite2-Country-CSV_20230526/'
GEO_Light_City = 'GeoLite2-City-CSV_20230526/'
GEO_Light_ASN = 'GeoLite2-ASN-CSV_20230526/'
# Reads Flow Analysis File
def Read_Flow_CSV():
    try:
        df1 = pd.read_csv(file_path + flow_analysis_file)
        print(Fore.GREEN + "Reading the Flow CSV has Passed!")
        return df1
    except Exception:
        print(Fore.RED + """Reading the Flow CSV has Failed!, make sure variables file_path, flow_analysis_file are correct.""")

def Country_Block_IPv4():
    try:
        # Reads Geolite-Country-Block-IPV4
        df2 = pd.read_csv(file_path_Geo + GEO_Light_Country + 'GeoLite2-Country-Blocks-IPv4.csv')

        # Makes a list of the column 'Peer IP Address' in Flow Analysis File
        Flow_Peer_IP_Column = df1['Peer IP Address']

        # Gets GeoLite2-Country column network subnets and makes it a list.
        Country_Column_Network = df2['network']

        # Gets GeoLite2-Country column geoname_id and makes it a list.
        Country_Column_geoname_id = df2['geoname_id']

        # Create a copy of df1
        df_copy = df1.copy()

        # Create an empty list to store geoname_ids
        matching_geoname_ids = []

        # Iterate over each entry in Flow_Peer_IP_Column of df1
        for entry in Flow_Peer_IP_Column:
            ip = ipaddress.ip_address(entry)
            matching_geoname_id = np.nan

            # Iterate over each entry in Country_Column_Network of df2
            for i, network_entry in enumerate(Country_Column_Network):
                network = ipaddress.ip_network(network_entry)

                # Check if the IP address is within the network
                if ip in network:
                    matching_geoname_id = Country_Column_geoname_id[i]
                    break

            matching_geoname_ids.append(matching_geoname_id)

        # Add the matching geoname_ids as a new column to df_copy
        df_copy['Matching_geoname_id'] = matching_geoname_ids

        print(Fore.Green + "Matching the 'Peer IP Address' to Country_IPV4_Block CSV has been passed!")
        return df2, df_copy
    except Exception:
        print(Fore.RED + """Matching has Failed!, make sure variables such as CSV_Version number are correct, along with file names.""")

def City_Block_IPV4():
    try:
        # Read GeoLite2-City-Blocks-IPv4.csv
        df3 = pd.read_csv(file_path + GEO_Light_City + 'GeoLite2-City-Blocks-IPv4.csv', dtype=object, low_memory=False)

        # Create an empty list to store the matching geo_city_ids
        matching_geo_city_ids = []

        # Iterate over each entry in 'Peer IP Address' column of df_copy
        for entry in df_copy['Peer IP Address']:
            ip = ipaddress.ip_address(entry)
            matching_geo_city_id = np.nan

            # Iterate over each entry in 'network' column of df3
            for i, network_entry in enumerate(df3['network']):
                network = ipaddress.ip_network(network_entry)

                # Check if the IP address is within the network
                if ip in network:
                    matching_geo_city_id = df3['geoname_id'][i]
                    break

            matching_geo_city_ids.append(matching_geo_city_id)

        # Add the matching geo_city_ids as a new column to df_copy
        df_copy['geo_city_id'] = matching_geo_city_ids

        # Replace NaN values in 'geo_city_id' column with a default value (e.g., -1)
        df_copy['geo_city_id'].fillna(-1, inplace=True)

        # Convert 'geo_city_id' column to int64
        df_copy['geo_city_id'] = df_copy['geo_city_id'].astype(np.int64)
        print(Fore.Green + "Matching the 'Peer IP Address', and  to City_IPV4_Block CSV has been passed!")
        return df3
    except Exception:
        print(Fore.RED + """Matching has Failed!, make sure variables such as CSV_Version number are correct, along with file names.""")



def GEO_City_And_Locations():
    try:
        # Read GeoLite2-City-Locations-en.csv
        df4 = pd.read_csv(file_path + GEO_Light_City + 'GeoLite2-City-Locations-en.csv')

        # Merge df_copy with df4 based on 'geo_city_id' and 'geoname_id'
        df_merged = pd.merge(df_copy, df4, left_on='geo_city_id', right_on='geoname_id', how='left')

        # Add the merged columns to df_copy
        df_copy['subdivision_1_name'] = df_merged['subdivision_1_name'].fillna('N/A')
        df_copy['subdivision_2_name'] = df_merged['subdivision_2_name'].fillna('N/A')
        df_copy['city_name'] = df_merged['city_name'].fillna('N/A')
        print(Fore.Green + "Matching the 'geo_city_id' to 'geoname_id' in GeoLite2-City-Locations-en.csv has been passed!")
        return df4
    except Exception:
        print(Fore.RED + """Matching has Failed!, make sure variables such as CSV_Version number are correct, along with file names.""")



def ASN_Block_IPV4():
    try:
        df5 = pd.read_csv(file_path + GEO_Light_ASN + 'GeoLite2-ASN-Blocks-IPv4.csv')

        # Create empty lists to store the matching autonomous system numbers and organizations
        matching_autonomous_system_numbers = []
        matching_autonomous_system_organizations = []

        # Iterate over each entry in 'Peer IP Address' column of df_copy
        for entry in df_copy['Peer IP Address']:
            ip = ipaddress.ip_address(entry)
            matching_autonomous_system_number = np.nan
            matching_autonomous_system_organization = np.nan

            # Iterate over each entry in 'network' column of df5
            for i, network_entry in enumerate(df5['network']):
                network = ipaddress.ip_network(network_entry)

                # Check if the IP address is within the network
                if ip in network:
                    matching_autonomous_system_number = df5['autonomous_system_number'][i]
                    matching_autonomous_system_organization = df5['autonomous_system_organization'][i]
                    break

            matching_autonomous_system_numbers.append(matching_autonomous_system_number)
            matching_autonomous_system_organizations.append(matching_autonomous_system_organization)

        # Add the matching autonomous system numbers and organizations as new columns to df_copy
        df_copy['autonomous_system_number'] = matching_autonomous_system_numbers
        df_copy['autonomous_system_organization'] = matching_autonomous_system_organizations
        print(Fore.Green + "Matching the 'Peer IP Address' to ASN_IPV4_Block CSV has been passed!")
        return df5
    except Exception:
        print(Fore.RED + """Matching has Failed!, Make sure ASN_IPV4_Block is in the correct path.""")

# Write the DataFrame to a new CSV file

df1 = Read_Flow_CSV()
df2, df_copy = Country_Block_IPv4()
df3 = City_Block_IPV4()
df4 = GEO_City_And_Locations()
df5 = ASN_Block_IPV4()


df_copy.to_csv('/home/joshua/Desktop/StealthWatch/matching_entries.csv', index=False)
