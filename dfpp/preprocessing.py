import io
import os
from datetime import datetime
import re
import pandas as pd
import numpy as np

from dfpp.utils import change_iso3_to_system_region_iso3, fix_iso_country_codes, add_country_code, add_region_code

downloads = os.path.join(os.path.expanduser("~"), "Downloads")


def add_region_code_to_csv():
    pass


def acctoi_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="INFRASTRUCTURE II EN")
    source_df = source_df.iloc[7:124]
    source_df = source_df.replace(['…', '-', 'x[16]'], [np.nan, np.nan, np.nan])
    source_df.rename(columns={"Unnamed: 0": "Country"}, inplace=True)
    return source_df


def bti_project_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_excel(io.BytesIO(bytes_data))
    source_df = source_df.iloc[6:124]
    source_df = source_df.replace(['…', '-', 'x[16]'], [np.nan, np.nan, np.nan])
    country_column_name = "Regions:\n1 | East-Central and Southeast Europe\n2 | Latin America and the Caribbean\n3 | West and Central Africa\n4 | Middle East and North Africa\n5 | Southern and Eastern Africa\n6 | Post-Soviet Eurasia\n7 | Asia and Oceania"
    source_df = source_df.rename(columns={country_column_name: "Country"})
    return source_df


def cpi_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_excel(io.BytesIO(bytes_data))
    source_df.rename(columns={"country": "Country", "iso3": "Alpha-3 code", "year": "Year"}, inplace=True)
    source_df["Year"] = source_df["Year"].apply(lambda x: datetime.strptime(str(x), '%Y'))
    return source_df


def cpia_rlpr_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_csv(os.path.join(downloads, "CPIA_RLPR.csv"))
    # source_df = pd.read_csv(io.BytesIO(bytes_data))
    source_df.replace("..", np.nan, inplace=True)
    source_df.rename(columns={"Country Name": "Country", "Country Code": "Alpha-3 code"}, inplace=True)
    return source_df


def cpia_spca_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_csv(os.path.join(downloads, "CPIA_SPCA.csv"))
    # source_df = pd.read_csv(io.BytesIO(bytes_data))
    source_df.rename(columns={"Country Name": "Country", "Country Code": "Alpha-3 code"}, inplace=True)
    return source_df


def cpia_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_csv(os.path.join(downloads, "CPIA.csv"))
    # source_df = pd.read_csv(io.BytesIO(bytes_data))
    source_df.replace("..", np.nan, inplace=True)
    source_df.rename(columns={"Country Name": "Country", "Country Code": "Alpha-3 code"}, inplace=True)
    return source_df


def cw_ndc_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_csv(os.path.join(downloads, "CW_NDC.csv"))

    # source_df = pd.read_csv(io.BytesIO(bytes_data))
    # source_df.rename(columns={"Country Name": "Country", "Country Code": "Alpha-3 code"}, inplace=True)

    def reorganize_number_ranges(text):
        pattern = re.compile(r'(-?\d+)-(-?\d+)')
        match = re.findall(pattern, text)
        if match:
            for m in match:
                text = text.replace(f"{m[0]}-{m[1]}", f"{m[0]} - {m[1]}")
        else:
            # print("{} do not have '-' symbol for ranges".format(text))
            pass
        return text

    def extract_minimum_number(text):
        numbers = re.findall(r"-?\d+\.?\d*", text)
        numbers = [float(x) for x in numbers]
        if numbers:
            min_num = min(numbers)
            return min_num
        else:
            return text

    def extract_number_with_suffix(text):
        numbers = re.findall(r"-?\d+\.?\d* Gg", text)
        if len(numbers) == 1:
            return numbers[0] + "CO"
        else:
            return text

    def get_number_count(text):
        return len(re.findall(r"-?\d+\.?\d*", text))

    def reorganize_floating_numbers(text):
        return re.sub(r'(?<!\d)\.(\d+)', r'0.\1', text)

    def extract_and_change(text):
        numbers = re.findall(r"-?\d+\.?\d*", text)
        if numbers:
            number = float(numbers[0])
            if ("ktGgCO" in text) or ("GgCO" in text):
                number /= 10 ** 3
            else:
                # print("{} are in standard form".format(text))
                pass
            return number
        else:
            # print("{} no number in the text".format(text))
            return text

    needed_columns = ["Country", "ISO", "M_TarYr", "M_TarA2", "M_TarA3", "NDC"]
    replacement_string_values = {
        ",": "",
        "MtCO2eq": "",
        "MtCO2e": "",
        "(Mt CO2-e)2": "",
        "Mt CO2": "",
        "kt CO2eq": "ktGgCO",
    }
    # ****clean column values - M_TarA2****
    # replace n/a to empty string
    source_df.replace("n/a", "", inplace=True)
    # drop the row if either one of column values are empty
    source_df.dropna(subset=["M_TarA2", "M_TarA3", "M_TarYr"], inplace=True)
    # drop the row if either one of column contains sub string "Not Specified"
    source_df = source_df[~source_df["M_TarA3"].str.contains("Not Specified")]
    source_df = source_df[~source_df["M_TarA2"].str.contains("Not Specified")]

    source_df["M_TarA2"] = source_df["M_TarA2"].str.replace("%", "")

    # 26-28 -> 26 - 28
    # because 28 will be minus if this transformation is not done
    source_df["M_TarA2"] = source_df["M_TarA2"].apply(reorganize_number_ranges)
    # If there are multiple values in M_TarA2 take the lower one
    source_df["M_TarA2"] = source_df["M_TarA2"].apply(extract_minimum_number)

    # ****clean column values - M_TarA3****
    for key, value in replacement_string_values.items():
        source_df["M_TarA3"] = source_df["M_TarA3"].str.replace(key, value, regex=False)
    # extract the numbers that has suffix " Gg"
    source_df["M_TarA3"] = source_df["M_TarA3"].apply(extract_number_with_suffix)
    # calculate how many numbers are in the "M_TarA3" column cells because we drop the rows that the number count is more than 1
    source_df["M_TarA3 Number Count"] = source_df["M_TarA3"].apply(get_number_count)
    # drop the rows that the number count is more than 1
    source_df = source_df[source_df["M_TarA3 Number Count"] == 1]
    # replace .23 with 0.23 otherwise it will recognize as number 23
    source_df["M_TarA3"] = source_df["M_TarA3"].apply(reorganize_floating_numbers)
    # extract the number and apply changes if the unit is not the standard one
    source_df["M_TarA3"] = source_df["M_TarA3"].apply(extract_and_change)

    source_df["M_TarA2"] = source_df["M_TarA2"].astype(float)
    source_df["M_TarA3"] = source_df["M_TarA3"].astype(float)

    source_df["NDC"] = source_df["M_TarA3"] * (1 + (source_df["M_TarA2"] / 100.0))
    source_df["ndc_date"] = pd.to_datetime(source_df["ndc_date"], format='%m/%d/%Y')
    source_df["M_TarYr"] = pd.to_datetime(source_df["M_TarYr"], format='%Y')
    # filter latest values
    source_df.sort_values("ndc_date", inplace=True, ascending=True)
    source_df.drop_duplicates(subset=["ISO"], keep="last", inplace=True)
    source_df.rename(columns={"ISO": "Alpha-3 code"}, inplace=True)
    return source_df


def cw_t2_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_csv(io.BytesIO(bytes_data))
    return source_df


def eb_wbdb_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_excel(os.path.join(downloads, "EB_WBDB.xlsx"), sheet_name="DB Data (As of DB20)", header=3)
    # source_df = pd.read_csv(io.BytesIO(bytes_data))
    group_df = source_df.groupby('DB Year')
    print(group_df.columns)
    group_column_mapping = {}
    year_col_mapping = {
        2016: '(DB17-20 methodology)',
        2014: '(DB15 methodology)',
        2010: '(DB10-14 methodology)'
    }
    # for year in group_df.groups.keys():
    #     for db_year in year_col_mapping.keys():
    #         if int(year) >= db_year:
    #             indicator_suffix = year_col_mapping[db_year]
    #             print(indicator_suffix)
    #             # if (" ".join([indicator_mapping[indicator], indicator_suffix])) not in group_column_mapping.keys():
    #             #     group_column_mapping[" ".join([indicator_mapping[indicator], indicator_suffix])] = []
    #             # group_column_mapping[" ".join([indicator_mapping[indicator], indicator_suffix])].append(year)
    #             # break
    # for key in group_column_mapping.keys():
    #     df = pd.concat(group_df.get_group(year) for year in group_column_mapping[key])
    #     df['DB Year'] = df['DB Year'].apply(lambda x: datetime.strptime(str(int(float(x))), '%Y'))
    # print(source_df.columns)
    return source_df[["Ease of doing business score (DB17-20 methodology)", "Year"]]


def fao_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="Data")
    source_df["Time_Detail"] = source_df["Time_Detail"].apply(lambda x: str(x).rsplit("-")[0])
    source_df["Time_Detail"] = source_df["Time_Detail"].apply(lambda x: datetime.strptime(str(x), '%Y'))
    return source_df


def ff_dc_ce_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="Data")
    source_df["TimePeriod"] = pd.to_datetime(source_df["TimePeriod"], format='%Y')
    source_df = source_df.groupby("Type of renewable technology").get_group("All renewables")
    return source_df


#
def ghg_ndc_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_csv(io.BytesIO(bytes_data))
    source_df = source_df.groupby(["Sector", "Gas"]).get_group(("Total including LUCF", "All GHG"))
    source_df = change_iso3_to_system_region_iso3(source_df, "Country")
    return source_df


def gii_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_csv(io.BytesIO(bytes_data))
    return source_df


def global_data_fsi_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_csv(io.BytesIO(bytes_data))
    year = source_df.iloc[0]["Year of Year"]
    return source_df


def global_findex_database_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="Data", header=0, skiprows=0)
    source_df.rename(columns={"Country name": "Country", 'Country code': "Alpha-3 code"}, inplace=True)
    source_df["Year"] = pd.to_datetime(source_df["Year"], format='%Y')
    return source_df


def global_pi_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_csv(io.BytesIO(bytes_data))
    return source_df


def eil_pe_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="7.3 Official indicator", skiprows=2)
    source_df.drop(source_df[source_df['Value'] == ".."].index, inplace=True)
    source_df.reset_index()
    source_df["TimePeriod"] = pd.to_datetime(source_df["TimePeriod"], format='%Y')
    return source_df


def ec_edu_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_excel(io.BytesIO(bytes_data), skiprows=10, nrows=202, usecols=[i for i in range(0, 12)])
    new_column_names = ['Country', 'Total', 'Footnote_total', 'Male', 'fn_male', 'Female', 'fn_female', 'Poorest',
                        'fn_poorest', 'Richest', 'fn_richest', 'Source']
    source_df.columns = new_column_names
    source_df.dropna(subset=['Source'], inplace=True)
    source_df.replace("-", np.nan, inplace=True)
    source_df.drop(source_df[source_df['Source'] == "MICS2"].index, inplace=True)
    source_df.reset_index(inplace=True)
    year = []
    for i in source_df['Source']:
        yr = re.findall(r'\d{4}', i)
        year.append(yr[0])

    source_df['Year'] = year
    source_df["Year"] = pd.to_datetime(source_df["Year"], format='%Y')
    return source_df


def hdr_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_csv(io.BytesIO(bytes_data))
    source_df = change_iso3_to_system_region_iso3(source_df, "Alpha-3 code")
    source_df.reset_index(inplace=True)
    fix_iso_country_codes(source_df, "Alpha-3 code", 'HDR')
    return source_df


def heritage_id_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_excel(io.BytesIO(bytes_data))
    source_df.rename(columns={"Country name": "Country", 'Country code': "Alpha-3 code"}, inplace=True)
    source_df["Year"] = pd.to_datetime(source_df["Year"], format='%Y')
    return source_df


def ilo_ee_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_csv(io.BytesIO(bytes_data))
    source_df = source_df.replace("none", np.nan)
    source_df["Year"] = source_df["Year"].apply(lambda x: datetime.strptime(str(x), '%Y'))
    return source_df


def ilo_lfs_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_excel(io.BytesIO(bytes_data), header=5)
    source_df["Time"] = source_df["Time"].apply(lambda x: datetime.strptime(str(x), '%Y'))
    return source_df


def ilo_nifl_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_excel(io.BytesIO(bytes_data), header=5)
    source_df["Time"] = source_df["Time"].apply(lambda x: datetime.strptime(str(x), '%Y'))
    return source_df


# def ilo_nifl_edu_transform_preprocessing(bytes_data: bytes = None):
#     source_df = pd.read_excel(io.BytesIO(bytes_data), header=5)
#     source_df["Time"] = source_df["Time"].apply(lambda x: datetime.strptime(str(x), '%Y'))
#     return source_df
#
#
# def ilo_nifl_rt_age_transform_preprocessing(bytes_data: bytes = None):
#     source_df = pd.read_excel(io.BytesIO(bytes_data), header=5)
#     source_df["Time"] = source_df["Time"].apply(lambda x: datetime.strptime(str(x), '%Y'))
#     return source_df
#
#
# def ilo_nifl_rt_eco_transform_preprocessing(bytes_data: bytes = None):
#     source_df = pd.read_excel(io.BytesIO(bytes_data), header=5)
#     source_df["Time"] = source_df["Time"].apply(lambda x: datetime.strptime(str(x), '%Y'))
#     return source_df


def imf_ifi_transform_preprocessing(bytes_data: bytes = None):
    # source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="financial assistance")
    # source_df2 = pd.read_excel(io.BytesIO(bytes_data), sheet_name="debt-relief")
    # source_df2.rename(columns={"country": "Country", "source": "Type of Emergency Financing",
    #                           "amount approved in SDR": "Amount Approved in SDR",
    #                           "amount approved in USD": "Amount Approved in US$",
    #                           "date of approval": "Date of Approval"}, inplace=True)
    # source_df = pd.concat([source_df, source_df2], ignore_index=True)
    # source_df['Date of Approval'] = source_df['Date of Approval'].apply(
    #     lambda x: datetime.strptime(str(x).rsplit(" ")[0], '%Y-%m-%d'))
    # source_df["Amount Approved in US$"] = source_df["Amount Approved in US$"].apply(
    #     lambda x: float(x.rsplit("mill")[0].replace(",", "").replace("US$", "")) * (10 ** 6))
    #
    # return source_df
    pass


def imf_weo_baseline_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_excel(bytes_data)
    source_df.replace("--", np.nan, inplace=True)
    return source_df


def imf_weo_gdp_transform_preprocessing(bytes_data: bytes = None):
    # source_df = pd.read_excel(io.BytesIO(bytes_data))
    # source_df.replace("--", np.nan, inplace=True)
    # return source_df
    pass


def imf_weo_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_excel(io.BytesIO(bytes_data))
    source_df.replace("--", np.nan, inplace=True)
    return source_df


def iec_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_excel(io.BytesIO(bytes_data), header=1)
    source_df.dropna(axis=1, how='all', inplace=True)
    new_column_names = ['Country', 'Energy Type', 'On/Off grid', 'Year', 'Value']
    source_df.columns = new_column_names
    source_df = source_df.fillna(method="ffill")
    source_df.drop(source_df[source_df['Value'] == ".."].index, inplace=True)
    source_df["Year"] = pd.to_datetime(source_df["Year"], format='%Y')
    return source_df


def imsmy_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="Table 1", header=10)
    source_df.replace("..", np.nan, inplace=True)
    source_df.rename(columns={"Region, development group, country or area": "Country"}, inplace=True)
    source_df["Country"] = source_df["Country"].apply(lambda x: x.replace(" ", "").replace("*", ""))
    source_df["Year"] = source_df["Year"].apply(lambda x: datetime.strptime(str(x), '%Y'))
    source_df = add_country_code(source_df, "Country")
    source_df = add_region_code(source_df, "Country")
    return source_df


def inequality_hdi_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="Table 3", header=4)
    source_df = source_df.iloc[1:200]
    source_df = source_df.replace("..", np.nan)
    return source_df


def isabo_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_json(io.BytesIO(bytes_data))
    source_df = pd.json_normalize(source_df["table1"])
    source_df["year"] = source_df["year"].astype(float)
    source_df.dropna(inplace=True, subset=["year"])
    source_df.replace("NaN", np.nan, inplace=True)
    source_df["year"] = source_df["year"].apply(lambda x: datetime.strptime(str(round(x)), '%Y'))
    return source_df


def itu_ict_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="i99H", header=5)
    return source_df


def mdp_bpl_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_excel(io.BytesIO(bytes_data))
    source_df = source_df.iloc[1:]
    source_df["countryname"] = source_df["countryname"].apply(lambda x: " ".join(x.rsplit(" ")[1:]))
    source_df["period"] = source_df["period"].apply(lambda x: datetime.strptime(str(x).rsplit(" ")[-1], '%Y'))


# def mdp_indicators_taf_transform_preprocessing(bytes_data: bytes= None):
#     source_df = pd.read_json(io.BytesIO(bytes_data))
#     source_df

def mdp_transform_preprocessing(bytes_data: bytes = None):
    def mdp_metadata():
        return 'data'.encode('utf-8')

    country_df = pd.read_json(io.BytesIO(mdp_metadata()))
    country_df = country_df[["id", "name"]]
    country_df.set_index("id", inplace=True)
    source_df = pd.read_json(io.BytesIO(bytes_data))
    source_df = source_df[["c", "v", "t"]]
    source_df.set_index("c", inplace=True)
    source_df = source_df.join(country_df)
    source_df.reset_index(inplace=True)
    source_df["t"] = source_df["t"].apply(lambda x: datetime.strptime(str(x), '%Y'))
    return source_df


def natural_capital_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_excel(io.BytesIO(bytes_data))
    source_df["Total"] = source_df["Renewable natural capital"] + source_df["Nonrenewable natural capital"]
    return source_df


def nature_co2_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_excel(io.BytesIO(bytes_data), header=1)
    source_df["percent.6"] = source_df["percent.6"].apply(lambda x: x * 100)
    return source_df


def nd_climate_readiness_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_csv(io.BytesIO(bytes_data))
    non_values_columns = ["ISO3", "Name"]
    for column_name in source_df.columns:
        if column_name in non_values_columns:
            continue
        else:
            source_df[column_name + "_rank"] = source_df[column_name].rank(method="dense", ascending=False)
    return source_df


def oecd_raw_mat_consumption_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_csv(io.BytesIO(bytes_data))
    source_df = source_df.dropna(subset=['Value'])
    source_df['TIME'] = source_df['TIME'].apply(lambda x: datetime.strptime(str(x), '%Y'))
    return source_df


def owid_energy_data_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_csv(io.BytesIO(bytes_data))
    source_df.dropna(subset=["year"], inplace=True)
    source_df["year"] = source_df["year"].apply(lambda x: datetime.strptime(str(round(float(x))), '%Y'))
    source_df = add_region_code(source_df, "country", "iso_code")
    return source_df


def owid_export_transform(bytes_data: bytes = None):
    source_df = pd.read_csv(io.BytesIO(bytes_data))
    source_df.dropna(subset=["Year"], inplace=True)
    source_df["Year"] = source_df["Year"].apply(lambda x: datetime.strptime(str(round(float(x))), '%Y'))
    source_df = add_region_code(source_df, "Entity", "Code")
    return source_df


def owid_oz_consumption_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_csv(io.BytesIO(bytes_data))
    summation_column_names = [
        "Consumption of controlled substance (zero-filled) - Chemical: Methyl Chloroform (TCA)",
        "Consumption of controlled substance (zero-filled) - Chemical: Methyl Bromide (MB)",
        "Consumption of controlled substance (zero-filled) - Chemical: Hydrochlorofluorocarbons (HCFCs)",
        "Consumption of controlled substance (zero-filled) - Chemical: Carbon Tetrachloride (CTC)",
        "Consumption of controlled substance (zero-filled) - Chemical: Halons",
        "Consumption of controlled substance (zero-filled) - Chemical: Chlorofluorocarbons (CFCs)",
    ]
    source_df["Year"] = pd.to_datetime(source_df["Year"], format="%Y")
    source_df["Chemical Total"] = source_df[summation_column_names].sum(axis=1)
    return source_df


def owid_t3_transform_preprocessing(bytes_data: bytes = None):
    indicator_mapping = {
        "pollution_deaths_fossil_fuels_owid": "Excess mortality from fossil fuels",
    }
    time_column_mapping = {
        "OWID_AIR": "Year",
    }
    time_format_mapping = {
        "OWID_AIR": "%Y",
    }
    country_name_mapping = {
        "OWID_AIR": "Entity",
    }
    country_iso3_mapping = {
        "OWID_AIR": "Code",
    }
    source_df = pd.read_csv(io.BytesIO(bytes_data))
    source_df.dropna(subset=[time_column_mapping["OWID_T3"]], inplace=True)
    source_df[time_column_mapping["OWID_T3"]] = pd.to_datetime(source_df[time_column_mapping["OWID_T3"]],
                                                               format=time_format_mapping["OWID_T3"])
    return source_df


def owid_trade_transform_preprocessing(bytes_data: bytes = None):
    country_col_name = 'Entity'
    iso_col_name = 'Code'
    source_df = pd.read_csv(io.BytesIO(bytes_data))
    source_df.dropna(subset=["Year"], inplace=True)
    source_df["Year"] = source_df["Year"].apply(lambda x: datetime.strptime(str(round(float(x))), '%Y'))
    source_df = add_region_code(source_df, country_col_name, iso_col_name)
    return source_df


def oxcgrt_rl_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_csv(io.BytesIO(bytes_data))
    source_df["Date"] = source_df["Date"].apply(lambda x: datetime.strptime(str(x), '%Y%m%d'))
    source_df["StringencyIndex_Average_ForDisplay"].update(source_df["StringencyIndex_Average"])
    source_df["StringencyIndex_Average"] = source_df["StringencyIndex_Average_ForDisplay"]
    return source_df


def pts_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_csv(io.BytesIO(bytes_data))
    source_df["Year"] = source_df["Year"].apply(lambda x: datetime.strptime(str(x), '%Y'))
    source_df.replace("NA", np.nan, inplace=True)
    return source_df


def sdg_mr_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="Table format")
    return source_df


def sdg_rap_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_excel(io.BytesIO(bytes_data), header=5)
    source_df["Time"] = source_df["Time"].apply(lambda x: datetime.strptime(str(x), '%Y'))
    return source_df


def sipri_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="Share of Govt. spending", header=7)
    source_df.replace(["..."], [np.nan], inplace=True)
    return source_df


def sme_transform_preprocessing(bytes_data: bytes = None):
    # def get_float_values(row, divident, devisor):
    #     devisor_val = []
    #     row.replace(" ", np.nan, inplace=True)
    #     if type(row[divident]) == type("str"):
    #         divident_val = float(row[divident].replace(",", "").replace(" ", "").replace(":", ""))
    #     else:
    #         divident_val = row[divident]
    #     for col in devisor:
    #         if type(row[col]) == type("str"):
    #             devisor_val.append(float(row[col].replace(",", "").replace(" ", "").replace(":", "")))
    #         else:
    #             devisor_val.append(row[col])
    #     return (divident_val, devisor_val
    #             )
    #
    # source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="Time Series")
    # source_df.replace(":", np.nan, inplace=True)
    # source_df = source_df.iloc[1:]
    # source_df.reset_index(inplace=True)
    # source_df.rename(columns={"Country": "Country", "Country Code": "Alpha-3 code"}, inplace=True)
    # source_unique_df = source_df.drop_duplicates(subset=["Alpha-3 code"], keep="last").set_index("Alpha-3 code")[
    #     ["Country", ]]
    # cols = list(set(source_df.columns))
    # group_df = source_df.groupby("Alpha-3 code")
    # return source_df
    pass


def undp_gii_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_excel(io.BytesIO(bytes_data), header=5)
    source_df = source_df.iloc[2:202]
    source_df.replace("..", np.nan, inplace=True)
    return source_df


def undp_hdi_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_csv(io.BytesIO(bytes_data))
    return source_df


def undp_mpi_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_excel(io.BytesIO(bytes_data), header=4, sheet_name="Table 2")
    source_df = source_df.iloc[:188]
    region_source_df = pd.read_excel(io.BytesIO(bytes_data), header=4, sheet_name="Table 1")
    start = region_source_df[region_source_df['Country'] == 'Developing countries'].index[0]
    end = region_source_df[region_source_df['Country'] == 'Notes'].index[0]
    region_source_df = region_source_df.iloc[start:end - 2]
    region_source_df_sub = region_source_df[['Country', 'Value']]
    region_source_df_sub.rename(columns={'Country': 'Unnamed: 0'}, inplace=True)
    print(region_source_df.columns[2].split('-')[-1])
    region_source_df_sub['Unnamed: 2'] = region_source_df.columns[2].split('-')[-1]
    source_df = source_df.append(region_source_df_sub)
    source_df["Unnamed: 2"] = source_df["Unnamed: 2"].apply(lambda x: x.rsplit(" ")[0].rsplit("/")[-1])
    source_df["Unnamed: 2"] = source_df["Unnamed: 2"].apply(lambda x: datetime.strptime(str(x), '%Y'))
    return source_df


def unescwa_fr_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="Sheet 1")
    source_df['Government fiscal support (Bn USD) 2020 & 2021'] = source_df[
        'Government fiscal support (Bn USD) 2020 & 2021'].apply(lambda x: x * (10 ** 9))
    return source_df


def unicef_dev_ontrk_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_csv(io.BytesIO(bytes_data))
    source_df["TIME_PERIOD"] = source_df["TIME_PERIOD"].apply(lambda x: datetime.strptime(str(x), '%Y'))
    source_df.replace(['No data'], [np.nan], inplace=True)
    source_df = change_iso3_to_system_region_iso3(source_df, "REF_AREA")
    return source_df


def untp_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="Estimates", header=16)
    source_df.dropna(subset=["Year"], inplace=True)
    source_df["Year"] = source_df["Year"].apply(lambda x: datetime.strptime(str(round(float(x))), '%Y'))
    source_df["Total Population, as of 1 January (thousands)"] = source_df[
        "Total Population, as of 1 January (thousands)"].apply(lambda x: x * 1000)
    source_df = add_region_code(source_df, "Region, subregion, country or area *", "ISO3 Alpha-code")
    return source_df


def vdem_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_csv(io.BytesIO(bytes_data))
    source_df["year"] = source_df["year"].apply(lambda x: datetime.strptime(str(x), '%Y'))
    return source_df


def time_udc_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_csv(io.BytesIO(bytes_data), encoding='ISO-8859-1', skiprows=1)
    source_df[" Period"] = pd.to_datetime(source_df[" Period"], format='%Y')
    return source_df


def wbank_access_elec_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_excel(bytes_data, sheet_name="Data", header=0)
    source_df.rename(columns={'Country Name': 'Country', 'Country Code': 'Alpha-3 code'}, inplace=True)
    return source_df


def wbank_info_eco_transform_preprocessing(bytes_data: bytes = None, sheet_name=None):
    source_df = pd.read_excel(bytes_data, sheet_name=sheet_name, header=0)
    source_df.rename(columns={"Economy ": "Country", "Code": "Alpha-3 code"}, inplace=True)
    return source_df


def wbank_info_transform_preprocessing(bytes_data: bytes = None, sheet_name=None):
    source_df = pd.read_excel(bytes_data, sheet_name=sheet_name, header=13)
    column_rename = {}
    for column in source_df.columns:
        column_rename[column] = str(column) + "_" + source_df.iloc[0][column]
    source_df.rename(columns=column_rename, inplace=True)
    source_df = source_df.iloc[1:]
    return source_df


def wbank_poverty_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_excel(bytes_data, header=0, skiprows=3)
    source_df = source_df[['code', 'economy', 'mdpoor_i1', 'year']]
    source_df.dropna(inplace=True)
    column_rename = {}
    for column in source_df.columns:
        if column == 'mdpoor_i1':
            column_rename[column] = 'Multidimensional poverty headcount ratio (%)'
        elif column == 'economy':
            column_rename[column] = 'Country'
        elif column == 'code':
            column_rename[column] = 'Alpha-3 code'
        else:
            column_rename[column] = str(column)

    source_df.rename(columns=column_rename, inplace=True)

    # Convert year column datatype to a datetime object
    source_df["year"] = pd.to_datetime(source_df["year"], format='%Y')
    return source_df


def wbank_energy_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_excel(io.BytesIO(bytes_data), sheet_name="Data", header=0)
    column_rename = {}
    i = 0
    for column in source_df.columns:
        if i > 3:
            column_rename[column] = str(column)[:4]
        else:
            if column == "Country Name":
                column_rename[column] = "Country"
            elif column == "Country Code":
                column_rename[column] = "Alpha-3 code"
            else:
                column_rename[column] = str(column)
        i = i + 1
    source_df.rename(columns=column_rename, inplace=True)
    print(source_df)
    source_df = source_df.replace("..", np.NaN)
    return source_df


def wbank_rai_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_excel(bytes_data, sheet_name="RAI Crosstab", header=2)
    source_df.columns = [f"year_{col}" if isinstance(col, int) else col for col in source_df.columns]
    source_df = source_df.dropna(how='all')
    return source_df


def wbank_t1_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_excel(bytes_data, sheet_name="Data", header=3)
    return source_df


def wbdb_transform_preprocessing(bytes_data: bytes = None):
    # source_df = pd.read_excel(bytes_data)
    # source_df["Economy"] = source_df["Economy"].apply(lambda x: x.rsplit("*")[0] if '*' in str(x) else x)
    # source_df.dropna(subset=["Year"], inplace=True)
    # source_df["Year"] = source_df["Year"].apply(lambda x: datetime.strptime(str(int(x)), '%Y'))
    # return source_df
    pass


def who_global_rl_transform_preprocessing(bytes_data: bytes = None):
    # source_df = pd.read_csv(io.BytesIO(bytes_data))
    # source_df["Year"] = source_df["Year"].apply(lambda x: datetime.strptime(str(int(x)), '%Y'))
    # return source_df
    pass


def who_pre_edu_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_json(io.BytesIO(bytes_data))
    source_df = source_df["value"]
    source_df = pd.json_normalize(source_df)
    source_df["TimeDim"] = source_df["TimeDim"].apply(lambda x: datetime.strptime(str(x), '%Y'))
    pivot_df = source_df.pivot_table(
        values='NumericValue',
        index=['SpatialDim', 'TimeDim'],
        columns='Dim1'
    ).reset_index()
    pivot_df.replace(['No data'], [np.nan], inplace=True)
    pivot_df = change_iso3_to_system_region_iso3(pivot_df, "SpatialDim")
    return pivot_df


def who_rl_transform_preprocessing(bytes_data: bytes = None):
    source_df = pd.read_json(io.BytesIO(bytes_data))
    source_df = source_df["value"]
    source_df = pd.json_normalize(source_df)
    source_df.replace(['No data'], [np.nan], inplace=True)
    source_df["TimeDim"] = source_df["TimeDim"].apply(lambda x: datetime.strptime(str(x), '%Y'))
    source_df = change_iso3_to_system_region_iso3(source_df, "SpatialDim")
    return source_df


# def check_existence_of_functions():
#     import sys
#     indicator_csv = pd.read_csv("migration/test/indicator_list.csv")
#     function_list = [func for func in dir(sys.modules[__name__]) if callable(getattr(sys.modules[__name__], func))]
#     missing_functions = [func for func in indicator_csv["preprocessing"].to_list() if func not in function_list]
#
#     # missing_functions = [func for func in function_list if func not in indicator_csv["preprocessing"].to_list()]
#     def find_ilo():
#         iloz = []
#         for func in missing_functions:
#             if 'ilo' not in func:
#                 iloz.append(func)
#         return iloz
#
#     undone_list = list(set(find_ilo()))
#     print(undone_list, len(undone_list))


if __name__ == "__main__":
    pass
    # check_existence_of_functions()
    # print(df.head())
