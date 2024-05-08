import os
import asyncio
import logging
import pathlib
import glob
from configparser import ConfigParser

import pandas as pd
from dotenv import load_dotenv

from dfpp.common import cfg2dict, dict2cfg
from dfpp.run_transform import transform_sources, run_transformation_for_indicator
from dfpp.storage import StorageManager

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
load_dotenv()
ROOT_FOLDER = os.getenv("ROOT_FOLDER")


async def upload_source_cfgs(source_cfgs_path: pathlib.Path) -> None:
    async with StorageManager(
            connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
            container_name=os.getenv("AZURE_STORAGE_CONTAINER_NAME"),
            root_folder=os.getenv("ROOT_FOLDER")
    ) as storage_manager:
        for root, dirs, files in os.walk(source_cfgs_path):
            for file in files:
                if file.endswith(".cfg"):
                    logger.info(f"Uploading {file} to Azure Blob Storage")
                    await storage_manager.upload(
                        dst_path=os.path.join(ROOT_FOLDER, "config", "sources", root.split('/')[-1], file),
                        src_path=os.path.join(root, file),
                        data=file.encode("utf-8"),
                        content_type="text/plain"
                    )
                    logger.info(f"Uploaded {file} to Azure Blob Storage")


async def upload_indicator_cfgs(indicator_cfgs_path: pathlib.Path) -> None:
    async with StorageManager(
            connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
            container_name=os.getenv("AZURE_STORAGE_CONTAINER_NAME"),
            root_folder=os.getenv("ROOT_FOLDER")
    ) as storage_manager:
        for root, dirs, files in os.walk(indicator_cfgs_path):
            for file in files:
                if file.endswith(".cfg"):
                    logger.info(f"Uploading {file} to Azure Blob Storage")
                    await storage_manager.upload(
                        dst_path=os.path.join(ROOT_FOLDER, "config", "indicators", file),
                        src_path=os.path.join(root, file),
                        data=file.encode("utf-8"),
                        content_type="text/plain"
                    )
                    logger.info(f"Uploaded {file} to Azure Blob Storage")


def count_number_of_outputs(path):
    file_path = glob.glob(f"{path}/*.json")
    files = [file for file in file_path]
    return len(files)


async def create_indicator_source_mapping():
    async with StorageManager(
            connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
            container_name=os.getenv("AZURE_STORAGE_CONTAINER_NAME"),
            root_folder=os.getenv("ROOT_FOLDER")
    ) as storage_manager:

        indicator_cfgs = await storage_manager.get_indicators_cfg()
        indicator_source_list = []
        for indicator_cfg in indicator_cfgs:
            source_id = indicator_cfg['indicator']['source_id']
            if source_id == "WBANK_INFO_VA_ESTIMATE" or source_id == "WBANK_INFO_VA_RANK":
                continue
            source_cfg = await storage_manager.get_source_cfg(source_id=source_id)
            source_url = source_cfg['source']['url']
            try:
                print(f"Adding indicator {indicator_cfg['indicator']['indicator_id']} with source {source_id}")
                indicator_source_list.append({
                    'indicator': indicator_cfg['indicator']['indicator_id'],
                    'source': indicator_cfg['indicator']['source_id'],
                    'source_url': source_url
                })
            except Exception as e:
                logger.error(e)

        data = pd.DataFrame(indicator_source_list)
        data.to_csv("indicator_source_mapping.csv", index=False)
        return indicator_source_list


def correct_hdi_indicators():
    indicator_list_path = "/home/thuha/Documents/indicator_list.csv"
    local_indicator_cfg_path = "/home/thuha/Desktop/Group5/indicators"
    indicator_df = pd.read_csv(indicator_list_path)
    for index, row in indicator_df.iterrows():
        indicator_id = row['Indicator ID']
        indicator_source = row['Source ID']
        parser = ConfigParser(interpolation=None)
        parser.read(f"{local_indicator_cfg_path}/{indicator_id}.cfg")
        indicator_dict = cfg2dict(parser)
        if indicator_dict.get('indicator') is None:
            print(f"Indicator {indicator_id} does not exist")
            continue
        indicator_dict['indicator']['source_id'] = indicator_source
        return_parser = dict2cfg(indicator_dict)
        with open(f"dict2cfg/{indicator_id}.cfg", "w") as f:
            return_parser.write(f)


def test_download():
    indicator_list_path = "/home/thuha/Documents/indicator_list.csv"
    sources_cfg_path = "/home/thuha/Desktop/Group5/sources"
    indicator_df = pd.read_csv(indicator_list_path)
    skip_sources = ["HDI", "VDEM", "GLOBAL_FINDEX_DATABASE"]
    for index, row in indicator_df.iterrows():
        indicator_id = row['Indicator ID']
        source_id = row['Source ID']
        parser = ConfigParser(interpolation=None)
        parser.read(f"{sources_cfg_path}/{source_id.lower()}/{source_id.lower()}.cfg")
        source_dict = cfg2dict(parser)
        # print(source_dict.get('source').get('id'))
        if source_dict.get('source').get('id') in skip_sources or 'ILO' in source_dict.get('source').get('id'):
            # print(f"Source {source_id} does not exist")
            continue
        source_url = source_dict['source']['url']
        download_function = source_dict['source']['downloader_function']
        print(f"downloading for indicator {indicator_id} with source {source_id}")
        download_cmd = f"python -m dfpp.cli run download -i {indicator_id}"
        os.system(download_cmd)
        print(f"processed for indicator {indicator_id} with source {source_id}")


# def test_sources_codes():
#     source_list_path = "/home/thuha/Documents/source_list.csv"
#     source_df = pd.read_csv(source_list_path)
#     for index, row in source_df.iterrows():
#         source_id = row['Source ID']
#         source_cmd = f"python -m dfpp.cli run download -s {source_id}"
#         sources = os.system(source_cmd)

file_names = [
    "MEN_SS.csv",
    "SW_PSE.csv",
    "SW_HIVPPC.csv",
    "MEN_AHC_SD.csv",
    "HIV_P_MEN.csv",
    "PID_PSE.csv",
    "PID_HIVP.csv",
    "PID_NI.csv",
    "PID_AHC_SD.csv",
    "PID_SIP.csv",
    "PID_HIVC.csv",
    "PID_HIV_TSA.csv",
    "TRANS_GP.csv",
    "SIPRI.xlsx",
    "HERITAGE_ID.xls",
    "SDG_MR.xlsx",
    "ISABO.json",
    "SDG_RAP.xlsx",
    "TECH_ICT.csv",
    "FAO_FOREST_AREA_LONG_TERM_MANAGEMENT.csv",
    "FAO_FOREST_AREA_PROTECTED_AREAS.csv",
    "UNDP_CLIMATE_CHANGE_DEATH_RATE_85.csv",
    "UNDP_CLIMATE_CHANGE_DEATH_RATE_45.csv",
    "AP_MATT_PE.csv",
    "HAP_SFUEL.csv",
    "WDI.csv",
    "FRST_AREA.csv",
    "TRRPROT_AREA.csv",
    "LGBTQ_EQUALITY.xlsx",
    "DDDF_IHME.csv",
    "DDDM_IHME.csv",
    "PCDF_IHME.csv",
    "PCDM_IHME.csv",
    "DCDF_IHME.csv",
    "DCDM_IHME.csv",
    "PCRDF_IHME.csv",
    "PCRDM_IHME.csv",
    "DCRDF_IHME.csv",
    "DCRDM_IHME.csv",
    "PDNF_IHME.csv",
    "PDNM_IHME.csv",
    "DNDF_IHME.csv",
    "DNDM_IHME.csv",
    "PONCF_IHME.csv",
    "PONCDM_IHME.csv",
    "DONCDF_IHME.csv",
    "DONCDM_IHME.csv",
    "UNAIDS.csv",
    "WBANK_HIV.csv",
    # "OWID_EXPORT_DATA.csv",
    # "OWID_TRADE_DATA.csv",
    # "OWID_AIR.csv",
    # "OWID_OZ_CONSUMPTION.csv",
    # "ACCTOI.xlsx",
    # "NATURE_CO2.xlsx",
    # "NATURAL_CAPITAL.xlsx",
]


async def copy_raw_sources(source_path=None, dst_path=None):
    async with StorageManager(
            connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
            container_name=os.getenv("AZURE_STORAGE_CONTAINER_NAME"),
            root_folder="DataFuturePlatform/"
    ) as storage_manager:
        for file in file_names:
            raw_path = storage_manager.ROOT_FOLDER + "Sources/Raw/" + file
            # download
            await storage_manager.download(blob_name=raw_path,
                                           dst_path=f"/home/thuha/Desktop/Group5/sources/raw/{file}")

            try:
                # upload
                await storage_manager.upload(
                    dst_path=storage_manager.SOURCES_PATH,
                    src_path=f"/home/thuha/Desktop/Group5/sources/raw/{file}",
                    data=file.encode("utf-8"),
                    content_type="text/plain"
                )
            except Exception as e:
                logger.error(e)
                continue


transformed_indicators = ['condomdistributiontoprisoners_transgp', 'coalconsumption_owided', 'abortion_abrtlr',
                          'changeinpovertygapduetooutofpockethealthspending$3.2povertyline_hefpi',
                          'Informalemploymentbysexthousandsmale_ilolfs', 'civilliberties_vdem',
                          'bedsinmentalhospitals_bedsmh', 'Informalemploymentbysexthousandstotal_ilolfs',
                          'access_elec_rural_wb', 'changeinabsolutenumberofpeopleexposedtomalariaanddengue_informcc',
                          'changeinabsolutenumberofpeopleexposedtodrought_informcc',
                          'condomdistributionperpersoninprevensionprogrammes_menss',
                          'antiretroviraltherapycoverageamongmenhavingsexwithmenhiv_menss',
                          'carbon_dioxide_emissions_per_capita_hdr', 'climate_risks_rank_cw',
                          'births_attended_by_skilled_health_personnel', 'central_govt_debt_pct_of_gdp_cpiacgd',
                          'chemicals_value_added_manufacturing_wb', 'access_elec_wb',
                          'consumption_of_controlled_substance_hydrochlorofluorocarbons_owid',
                          'account_ownership_at_a_financial_institution_or_with_a mobile_money_service_provider_percentrage_of_population_ages_15_plus',
                          'betterplacetolive_mdpbpl',
                          'changeinpovertygapduetooutofpockethealthspending$5.5povertyline_hefpi',
                          'climate_change_related_change_in_death_rate_per_100000_population_85',
                          'Informalemploymentbysexthousandsfemale', 'adolescentbirthratelatest_gii',
                          'coefficient_of_human_inequality_hdr', 'bedsformentalhealthingeneralhospital_bedsmhgh',
                          'access_elec_urban_wb', 'consumption_of_controlled_substance_total_owid',
                          'coalelectricity_owided', 'changeinhumanhazardandexposuretoconflict_informcc',
                          'avoidanceofhealthcareduetostigmaanddiscrimination_menahcsd',
                          'civilsocietyinfrastructurerobustness_vdem', 'attendanceearlychildedu_ecedu',
                          'accesstointernet_cpiaati', 'childrenorhouseholdsreceivingchildorfamilycashbenefits_ilospf',
                          'chemicalevents_chemevents',
                          'changeinpovertygapduetooutofpockethealthspending$1.9povertyline_hefpi',
                          'changeinabsolutenumberofpeopleexposedtocoastalfloods_informcc', 'climate_risks_index_cw',
                          'accesstosmallweapons_globalpi', 'adequacyofsocialsafetynetprograms_wdi',
                          'accesstowaterservices_cpiaatws', 'changeinabsolutenumberofpeopleexposedtofloods_informcc',
                          'ambientparticulatematterpollutionexposure_apmattpe',
                          'climate_change_related_change_in_death_rate_per_100000_population_45',
                          'civilsocietyparticipatoryenvironment_vdem',
                          'account_ownership_at_a_financial_institution_or_with_a_mobile_money_service_provider_percentage_of_population_ages_15_plus',
                          'access_fuel_tech_wb',
                          'employementoutsidetheformalsectorbysexandagethousandsfemale1564_ilopiflage',
                          'coverageofprotectedareasinrelationtomarineareas_ermrmmpa', 'electoraldemocracy_vdem',
                          'coverageofhivpreventionprogrammesamongpeopleinjecteddrugs_pidhivc',
                          'deathsattributedtononcommunicablediseasesmale_doncdmihme', 'dfet_population_average_wb',
                          'deathsattributedtoneurologicaldisordersfemale_dndfihme',
                          'employedcoveredintheeventofworkinjury_ilospf',
                          'deathsfrominternalorganisedconflict_globalpi',
                          'deathsattributedtononcommunicablediseasesfemale_doncdfihme',
                          'deathsattributedtocardiovasculardiseasesfemale_dcdfihme', 'difference_from_hdi_value_hdr',
                          'electricitygeneration_ireneg', 'deathsattributedtodigestivediseasesfemale_dddfihme',
                          'economicinequalitytotal_globaldatafsi', 'contraceptiveservices_cslr',
                          'easeofdoingbusinessscore_ebwbdb', 'currenthealthexpenditure_whoche',
                          'current_account_balance_pct_of_gdp_cpiacab',
                          'employementoutsidetheformalsectorbysexandagethousandsfemale15andabove_ilopiflage',
                          'demographicpressure_globaldatafsi',
                          'employementoutsidetheformalsectorbysexandagethousandsfemale25andabove_ilopiflage',
                          'digital_payment_total_percentage_age_15_plus', 'countrywisemilitaryexpenditure_sipri',
                          'deathsattributedtochronicrespiratorydiseasesmale_dcrdmihme', 'data_energy_imports_wb',
                          'degreeofimplementationofinternationalinstrumentsaimingtocombatillegalunreportedunregulatedfishing_ilfish',
                          'deathsattributedtocardiovasculardiseasesmale_dcdmihme',
                          'employementoutsidetheformalsectorbysexandagethousandsfemale1524_ilopiflage',
                          'economy_globaldatafsi', 'digital_payment_female_percent_age_15_plus',
                          'emissionsassociatedwithghgtarget_cwndc', 'covidresponsestringency_oxcgrt',
                          'economicfreedomglobalrank_heritageid', 'digital_payment_male_percent_age_15_plus',
                          'data_energy_consumption_wb',
                          'employementoutsidetheformalsectorbysexandagethousandsmale1524_ilopiflage',
                          'cumulativecases_whoglobal',
                          'employementoutsidetheformalsectorbysexandagethousandsmale15andabove_ilopiflage',
                          'deathsattributedtoneurologicaldisordersmale_dndmihme', 'emissionlevelsallghg_ghgndc',
                          'dge_wb', 'dailyco2emissionspercentagechange_natureco2', 'corruptionperceptionsindex_cpi',
                          'deathsattributedtochronicrespiratorydiseasesfemale_dcrdfihme',
                          'domestic_general_government_expenditure_on_hiv_aids_and_stds_as_perc_of_gdp',
                          'domestic_private_expenditure_on_hiv_aids_and_stds_as_perc_of_gdp',
                          'deathsattributedtodigestivediseasesmale_dddmihme', 'cumulativedeaths_whoglobal',
                          'employementoutsidetheformalsectorbysexandagethousandsmale1564_ilopiflage',
                          'employementoutsidetheformalsectorbysexandagethousandstotal25andabove_ilopiflage',
                          'employementoutsidetheformalsectorbysexandstatusinemploymentthousandstotalemployees_ilopiflste',
                          'employementoutsidetheformalsectorbysexandstatusinemploymentthousandsfemaleselfemployed_ilopiflste',
                          'employementoutsidetheformalsectorbysexthousandstotal_ilopiflnb',
                          'femaleunpaiddomesticcare_timeudc', 'expected_years_of_schooling_female_hdr',
                          'externalintervention_globaldatafsi', 'expeditureonhivandstdofche_stdexp',
                          'femalelaborforceparticipationrate_cpiaflfpr', 'estimatednumberofpeoplehwithhiv_whoephiv',
                          'experienceofstigmaanddiscriminationamongmenhadsexwithmen_menss',
                          'employmentinagriculture_cpiaeia',
                          'employementoutsidetheformalsectorbysexandstatusinemploymentthousandstotalnotclassified_ilopiflste',
                          'equalprotectionofrightsandfreedomsacrosssocialgroups_vdem',
                          'employementoutsidetheformalsectorbysexandstatusinemploymentthousandsfemaleemployees_ilopiflste',
                          'employmentinservices_cpiaeis',
                          'employementoutsidetheformalsectorbysexandagethousandsmale25andabove_ilopiflage',
                          'employmentinindustry_cpiaeii', 'externalpeace_globaldatafsi',
                          'factionalizedelites_globaldatafsi', 'enterprisesexcluded_iloee',
                          'employementoutsidetheformalsectorbysexandagethousandstotal1524_ilopiflage',
                          'employementoutsidetheformalsectorbysexandstatusinemploymentthousandsmaleemployees_ilopiflste',
                          'externalconflicts_globalpi',
                          'employementoutsidetheformalsectorbysexandstatusinemploymentthousandsmaleselfemployed_ilopiflste',
                          'employementoutsidetheformalsectorbysexandagethousandstotal15andabove_ilopiflage',
                          'financial_institution_account_percentage_ages_15_plus',
                          'employementoutsidetheformalsectorbysexandstatusinemploymentthousandsfemalenotclassified_ilopiflste',
                          'experienceofsexualphisicalviolenceamongmenhadsexwithmen_menss',
                          'employementoutsidetheformalsectorbysexandstatusinemploymentthousandsmalenotclassified_ilopiflste',
                          'expected_years_of_schooling_male_hdr', 'finalcialflows_ffdcce',
                          'externalsourcesoffundinghivandstdofexternalhealthexpenditure_stdexp',
                          'employementoutsidetheformalsectorbysexandstatusinemploymentthousandstotalselfemployed_ilopiflste',
                          'expected_years_of_schooling_hdr',
                          'employementoutsidetheformalsectorbysexthousandsfemale_ilopiflnb',
                          'employementoutsidetheformalsectorbysexandagethousandstotal1564_ilopiflage',
                          'energypercapita_owided', 'expenditureontertiaryeducation_cpiaeote',
                          'femaleearlyattendanceedu_ecedu',
                          'employementoutsidetheformalsectorbysexthousandsmale_ilopiflnb',
                          'inequality_in_life_expectancy_hdr', 'inequality_in_income_hdr', 'inequality_in_eduation_hdr',
                          'income_per_gdp_ilosdg', 'income_per_gdp_2021_estimate_ilosdg', 'impactofterrorism_globalpi',
                          'ififinancialresponsepackages_imfifi', 'humanrights_globaldatafsi',
                          'humanflightandbraindrain_globaldatafsi', 'humandevelopmentindex_undphdi',
                          'human_development_index_male_hdr', 'human_development_index_female_hdr',
                          'householdairpolutionfromsolidfuels_hapsfuel', 'homicidesperhundredthousandpeople_globalpi',
                          'hivtreatmentandcareservice_hivtcs',
                          'hivtestingandstatusawarenessamongpeopleinjecteddrugs_pidhivtsa',
                          'hivprevalenceamongtransgenderpeople_transgp', 'hivprevalenceamongprisoners_transgp',
                          'hivprevalenceamongmen_hivpmen', 'hivconfidentiality_hivconf',
                          'healthcare_facilities_no_waste_management_service_non_hospital',
                          'finantialcontributiononun_globalpi', 'healthcare_facilities_basic_waste_management_services',
                          'health_service_provision',
                          'guaranteeaccesstosexualreproductivehealthcareeducationinformation_srhlrcten',
                          'groupgrievance_globaldatafsi', 'gross_national_income_per_capita_male_hdr',
                          'gross_national_income_per_capita_hdr', 'gross_national_income_per_capita_female_hdr',
                          'governmentnetlendingbaseline_imfweobaseline', 'governmentnetlending_imfweo',
                          'governmentexpenditureontotaleducation_wdi', 'governmenteffectivenessrank_wbge',
                          'governmenteffectivenessestimate_wbge', 'governmentdebtbaseline_imfweobaseline',
                          'governmentdebt_imfweo', 'globalpeaceindexscore_globalpi', 'globalpeaceindexrank_globalpi',
                          'global_health_security_index', 'forest_area_wb', 'giniindex_cpiagi',
                          'gghedaspercentageofgge_gghedgge', 'genderinequalityindexlatest_gii', 'gendergap_timeudc',
                          'gender_development_index_hdr', 'gdppercapitapercentagechange_imfweo', 'gdppercapita_cpiagdp',
                          'gdpbaseline_imfweobaseline', 'gdp_imfweo', 'forestareatothepercentageoflandarea_frstarea',
                          'informalemploymentbysexandstatusinemploymentthousandsfemaleemployees_iloniflste',
                          'legislationonuniversalhealthcoverage_wholouhc',
                          'informalemploymentbysexandagethousandsmale25andabove_iloniflage',
                          'largeenterprisespercentageofallenterprises_sme',
                          'informalemploymentbysexandagethousandstotal1564_iloniflage', 'laboratory_ihr',
                          'informalsectorasbiggestobstacle_isabo',
                          'informalemploymentbysexandagethousandsmale1564_iloniflage',
                          'laborforcewithadvanced_education_cpialfae',
                          'informalemploymentbysexandstatusinemploymentthousandsmaleemployees_iloniflste',
                          'informalemploymentbysexandagethousandsfemale1524_iloniflage',
                          'informalemploymentbysexandagethousandsmale1524_iloniflage',
                          'informalemploymentbysexandagethousandsfemale25andabove_iloniflage',
                          'informalemploymentbysexandstatusinemploymentthousandsmaleselfemployed_iloniflste',
                          'informalemploymentbysexandstatusinemploymentthousandstotalselfemployed_iloniflste',
                          'largeenterprisesemploymentpercentageoftotal_sme', 'internalconflicts_globalpi',
                          'level _of_water_stress_wb', 'inflationbaseline_imfweobaseline',
                          'informalemploymentbysexandstatusinemploymentthousandstotalemployees_iloniflste',
                          'informalemploymentbysexandagethousandstotal15andabove_iloniflage', 'inflation_imfweo',
                          'internationalmigrantstock_imsmy', 'legislation_and_financing', 'infsizep_wb',
                          'informalemploymentbysexandagethousandstotal1524_iloniflage',
                          'internetusersofpopulation_ituict', 'laborforcewithadvanced education_cpialfae',
                          'informalemploymentbysexandstatusinemploymentthousandsfemaleselfemployed_iloniflste',
                          'level_of_water_stress_wb', 'informalemploymentbysexandagethousandsmale15andabove_iloniflage',
                          'informalemploymentbysexandagethousandsfemale15andabove_iloniflage',
                          'levelofviolentcrime_globalpi',
                          'informalemploymentbysexandstatusinemploymentthousandsmalenotclassified_iloniflste',
                          'informalemploymentbysexandagethousandstotal25andabove_iloniflage',
                          'informalemploymentbysexandstatusinemploymentthousandsnotclotalassified_iloniflste',
                          'labourforcemalelatest_gii', 'inequilityadjustedhdi_inequilityhdi',
                          'informalemploymentbysexandstatusinemploymentthousandsfemalenotclassified_iloniflste',
                          'levelofperceivedcriminality_globalpi',
                          'informalemploymentbysexandagethousandsfemale1564_iloniflage', 'labourforcefemalelatest_gii',
                          'installedelectricitycapacity_iec', 'multidimensionalpovertyindex_undpmpi',
                          'multidimensionalpovertyheadcount_ratio_wb', 'multidimensionalpovertyheadcount_ratio',
                          'multidimensionalpoverty_intensity_of_deprivation', 'msmespercentageofallenterprises_sme',
                          'mothertochildhivtransmissionrate_mchvitr',
                          'motherswithnewbornsreceivingmaternitybenefits_ilospf',
                          'mortalityattributedtononspreadingdiseasemale_sdgmr',
                          'mortalityattributedtononspreadingdiseasefemale_sdgmr',
                          'mortalityattributedtononspreadingdisease_sdgmr', 'monopolyonuseofforce_btiproject',
                          'moderateorseverefoodinsecurity_fao', 'mobilenetworkcoverage_covmn', 'mmrlatest_gii',
                          'militaryexpenditure_globalpi', 'militarisationindex_globalpi',
                          'migrantacceptanceindex_mdpma', 'microeterprisesemploymentpercentageoftotal_sme',
                          'national_health_emergency_framework', 'microenterprisespercentageofallenterprises_sme',
                          'menwhohadsexwithmenpopulationsizeestimate_menss',
                          'mentalhealthunitsingeneralhospitalsadmissions_mhugha',
                          'mentalhealthunitsingeneralhospitals_mhugh', 'mentalhealthoutpatientfacilities_mhof',
                          'mentalhealthdaytreatmentfacilities_mhdtf', 'mental_hospitals_per_100000',
                          'mediumenterprisespercentageofallenterprises_sme', 'mediumandhightechindustry_techict',
                          'meanhouseholdpercapitaoutofhealthspending_hefpi', 'mean_years_of_schooling_years_hdr',
                          'mean_years_of_schooling_male_years_hdr', 'mean_years_of_schooling_female_years_hdr',
                          'maternitycare_mclr', 'material_footprint_per_capita_tonnes_hdr',
                          'material_consumption_tonne_capita_oecd', 'mameemploymentpercentageoftotal_sme',
                          'maleunpaiddomesticcare_timeudc', 'malelaborforceparticipationrate_cpiamlpr',
                          'maleattendanceearlychildedu_ecedu', 'literacyratemale_cpialrm', 'literacyratefemale_cpialrf',
                          'lifesavingcommodities_lsclr', 'life_expectancy_at_birth_years_hdr',
                          'life_expectancy_at_birth_male_years_hdr', 'life_expectancy_at_birth_female_years_hdr',
                          'multidimensionalpovertyindex_value',
                          'percentage_of_forest_area_within_legally_established_protected_areas',
                          'percchangeinremittancefrompreviousyear_cpiacir',
                          'peopleinjecteddrugspopulationsizeestimate_pidpse', 'politicalterrorscale_pts',
                          'peopleinjecteddrugsneedlesperinjector_pidni', 'poorestattendace_ecedu',
                          'peopleinjecteddrugshivprevalence_pidhivp',
                          'populationbelowsixtyofmedianconsumptionpovertylinebyoutofpockethealthcareexpenditure_hefpi',
                          'peopleinjecteddrugsavoidanceofhealthcareduetostigmaanddiscrimination_pidahcsd',
                          'pollution_deaths_fossil_fuels_owid',
                          'people_denied_health_services_because_of_hiv_last_12_months',
                          'politicalinstability_globalpi', 'parlimentseatsfemale_gii',
                          'populationcoveredbyatleastonesocialprotectionbenefit_ilospf', 'overall_loss_hdr',
                          'populationages65andabove_cpiapa65a', 'organisedinternalconflict_globalpi',
                          'ores_metals_exports_wb', 'populationlivinginslums_cpiaplis',
                          'physiciansper10000people_whops',
                          'percentageofgirlsundergonefemalegenitalmalfunctionsagebetween15to49_fgmafa',
                          'personsaboveretirementagereceivingapensionmale_ilospf',
                          'percentageofgirlsundergonefemalegenitalmalfunctionsagebelow15_fgmua',
                          'personsaboveretirementagereceivingapension_ilospf', 'noofarmedpersonnel_globalpi',
                          'percentagechangeinremittancefrompreviousyear_cpiacir',
                          'poorpersonscoveredbysocialprotectionsystems_ilospf',
                          'personswithseveredisabilitiescollectingdisabilitysocialprotectionbenefits_ilospf',
                          'newbusinessdensityrate_nbbwbdb', 'offgridinstalledelectricitycapacity_iec',
                          'percentofterrestrialprotectedareasoftotallandareas_trrprotarea',
                          'nursesworkinginmentalhealthsector_nursmhs', 'nursesandmidwivesper10000people_whonam',
                          'numofpregnantwomenwithhivreceivedantiretrovirals_hivpwrapmtct',
                          'numofpregnantwomanwithhivneedingantiretrovirals_hivpwnpmtct', 'numberofnewllc_nbwbdb',
                          'populationusingbasicdrinkingwaterrural_cpiapbdwr',
                          'numberofhospitalbedsper10000people_whohb',
                          'numberofdeathsandmissingpersonsanddirectlyaffectedpeopleattributedtodisaster_ndmdsr',
                          'populationusingbasicdrinkingwaterurban_cpiapbdwu',
                          'planetary_pressures_adjusted_human_development_index_value_hdr',
                          'populationusingbasicdrinkingwater_cpiapbd', 'naturalcapital_naturalcapital',
                          'noofjailedpopulation_globalpi', 'populationusingbasicsanitationservices_cpiapbss',
                          'noofinternalsecurityofficers_globalpi', 'noofdeathsdueexternalorganisedconflict_globalpi',
                          'personsaboveretirementagereceivingapensionfemale_ilospf',
                          'povertyheadcountratioat1.90_cpiaphr19', 'ruleoflawestimatevallatest_wbankinfo',
                          'richestattendance_ecedu', 'resourcedistribution_vdem',
                          'renewable_internal_freshwater_resources_wb', 'remittanceaspercofGDP_cpiacir',
                          'remittanceas%ofGDP_cpiacir', 'relwithneighbouringcountries_globalpi',
                          'refugeesandinternalldisplacedpeople_globalpi', 'refugeesandidps_globaldatafsi',
                          'refugeepopulationbycountry_cpiarpbc', 'receivingapensiontotal_sdgrap',
                          'receivingapensionmale_sdgrap', 'receivingapensionfemale_sdgrap', 'readiness_rank_nd',
                          'readiness_index_nd', 'publicservices_globaldatafsi',
                          'publicsectormanagementinstitutions_cpiapsmi', 'publichealthriskcommunication_phrcomm',
                          'psychologistworkinginmentalhealthsector_psycholmhs',
                          'psychiatricsworkinginmentalhealthsector_psychiamhs', 'protectionofrightsandfreedoms_vdem',
                          'proportion_of_population_pushed_below_60_perc_median_consumption_poverty_line_by_out_of_pocket_expenditure',
                          'prop_children_dev_ontrk_unicef', 'problems_accessing_health_care_perc_of_women',
                          'prisonerspopulationsizeestimate_transgp', 'primaryschoolswithinternet_acctoi',
                          'primaryenergyintensity_eilpe', 'prevalenceofnoncommunicablediseasesmale_poncdmihme',
                          'prevalenceofnoncommunicablediseasesfemale_poncdfihme',
                          'prevalenceofneurologicaldisordersfemale_pdnfihme',
                          'prevalenceofdigestivediseasesmale_pddmihme', 'prevalenceofdigestivediseasesfemale_pddfihme',
                          'safeinjectingpractices_pidsip', 'prevalenceofchronicrespiratorydiseasesmale_pcrdmihme',
                          'prevalenceofchronicrespiratorydiseasesfemale_pcrdfihme',
                          'prevalenceofcardiovasculardiseasesmale_pcdmihme', 'povertyheadcountratioat3.20_cpiaphr32',
                          'prevalenceofcardiovasculardiseasesfemale_pcdfihme',
                          'prevalencealcoholandsubstanceusedisordersbothsexes_pasdis',
                          'prevalence_of_mental_health_disorders',
                          'prevalence_of_hiv_total_perc_of_population_ages_15_49_q1', 'pre_primary_learning_male_pct',
                          'pre_primary_learning_female_pct', 'pre_primary_learning_both_pct', 'powerdistribution_vdem',
                          'povertyheadcountratioat5.50_cpiaphr55',
                          'shareofemployementoutsidetheformalsectorbysexandstatusinemploymenttotalemployees_ilopiflrtste',
                          'shareoffemalebusinessowners_sofbwbdb',
                          'shareofemployementoutsidetheformalsectorbysexandstatusinemploymentmaleemployees_ilopiflrtste',
                          'shareofinformalemploymentbysexandagefemale25andabove_iloniflrtage',
                          'shareofemployementoutsidetheformalsectorbysexandagetotal25andabove_iloipflrtage',
                          'shareofemployementoutsidetheformalsectorbysexandagetotal15andabove_iloipflrtage',
                          'shareofemployementoutsidetheformalsectorbysexandagetotal1524_iloipflrtage',
                          'shareofemployementoutsidetheformalsectorbysexandagemale25andabove_iloipflrtage',
                          'shareofinformalemploymentbysexandagefemale1524_iloniflrtage',
                          'shareofemployementoutsidetheformalsectorbysexandagemale15andabove_iloipflrtage',
                          'shareofemployementoutsidetheformalsectorbysexandstatusinemploymentfemalenotclassified_ilopiflrtste',
                          'shareofemployementoutsidetheformalsectorbysexandagemale1524_iloipflrtage',
                          'shareofemployementoutsidetheformalsectorbysexthousandsfemale_ilopiflrt',
                          'shareofemployementoutsidetheformalsectorbysexandstatusinemploymentfemaleemployees_ilopiflrtste',
                          'secedufemalelatest_gii',
                          'shareofemployementoutsidetheformalsectorbysexandagefemale25andabove_iloipflrtage',
                          'shareofinformalemploymentbysexandagemale15andabove_iloniflrtage',
                          'shareofemployementoutsidetheformalsectorbysexandstatusinemploymentmaleselfemployed_ilopiflrtste',
                          'shareofinformalemploymentbysexandagemale1524_iloniflrtage',
                          'shareofemployementoutsidetheformalsectorbysexandagefemale15andabove_iloipflrtage',
                          'shareofemployementoutsidetheformalsectorbysexandstatusinemploymenttotalnotclassified_ilopiflrtste',
                          'shareofemployementoutsidetheformalsectorbysexandagefemale1524_iloipflrtage',
                          'share_of_seats_in_parliament_male_held_by_men_hdr', 'sexworkerspopulationsizeestimate_swpse',
                          'shareofemployementoutsidetheformalsectorbysexthousandsmale_ilopiflrt',
                          'sexworkerscoverageofpreventionprogrammes_swhivppc', 'sexualityeducation_selr',
                          'shareofinformalemploymentbysexandagefemale15andabove_iloniflrtage',
                          'shareofemployementoutsidetheformalsectorbysexandstatusinemploymenttotalselfemployed_ilopiflrtste',
                          'sexualandreproductivehealthlawsandregulations_srhlr',
                          'shareofinformalemploymentbysexandagetotal15andabove_iloniflrtage',
                          'shareoffemalesoleproprietors_sofswbdb', 'schoolenrollment_cpiasen',
                          'securityapparatus_globaldatafsi',
                          'shareofemployementoutsidetheformalsectorbysexandstatusinemploymentmalenotclassified_ilopiflrtste',
                          'shareofinformalemploymentbysexandagemale25andabove_iloniflrtage',
                          'shareofinformalemploymentbysexandagetotal1524_iloniflrtage',
                          'secondaryschoolswithinternet_acctoi', 'secedumalelatest_gii',
                          'shareofemployementoutsidetheformalsectorbysexandstatusinemploymentfemaleselfemployed_ilopiflrtste',
                          'shareoffemaledirectors_sofdwbdb',
                          'shareofemployementoutsidetheformalsectorbysexthousandstotal_ilopiflrt',
                          'unemployedreceivingunemploymentbenefits_ilospf', 'unemploymentbaseline_imfweobaseline',
                          'uhc_service_index_on_non_communicable_diseases',
                          'uhc_service_coverage_sub_index_on_service_capacity_and_access',
                          'uhc_service_coverage_sub_index_on_reproductive_maternal_newborn_and_child_death',
                          'uhc_service_coverage_sub_index_on_infectious_diseases', 'uhc_service_coverage_index',
                          'transgenderpeoplepopulationsizeestimate_transgp',
                          'transgederpeopleavoidanceofhealthcareduetostigmaanddiscrimination_transgp',
                          'trade_as_percentage_of_gdp', 'totalworkforceinmentalhealthsector_mhstotwf',
                          'totalurbanpopulation_cpiatup', 'totalruralpopulation_cpiatrp', 'totalpopulation_untp',
                          'totalgovernmentfiscalresponse_unescwafr', 'testdeathrate_whoglobal',
                          'tax_revenue_pct_of_gdp_cpiataxr', 'surveillancecapacity_survcap',
                          'suiciderateagestanderdized_srppas', 'structuralpoliciesclusteraverage_cpiaspca',
                          'strengthoflegalrightsindex_cpiaslri', 'statelegitimacy_globaldatafsi',
                          'solarelectricity_owided', 'socialsafetysecurityconflict_globalpi',
                          'socialinclusionequityclusteraverage_cpiasiea', 'unemployment_imfweo',
                          'smespercentageofallenterprises_sme', 'smallenterprisespercentageofallenterprises_sme',
                          'shareofinformalemploymentbysexthousandstotal_iloniflrt',
                          'shareofinformalemploymentbysexthousandsmale_iloniflrt',
                          'shareofinformalemploymentbysexthousandsfemale_iloniflrt',
                          'shareofinformalemploymentbysexandstatusinemploymenttotalselfemployed_iloniflrtste',
                          'shareofinformalemploymentbysexandstatusinemploymenttotalnotclassified_iloniflrtste',
                          'shareofinformalemploymentbysexandstatusinemploymenttotalemployees_iloniflrtste',
                          'shareofinformalemploymentbysexandstatusinemploymentmaleselfemployed_iloniflrtste',
                          'shareofinformalemploymentbysexandstatusinemploymentmalenotclassified_iloniflrtste',
                          'shareofinformalemploymentbysexandstatusinemploymentmaleemployees_iloniflrtste',
                          'shareofinformalemploymentbysexandagetotal25andabove_iloniflrtage',
                          'shareofinformalemploymentbysexandstatusinemploymentfemaleselfemployed_iloniflrtste',
                          'shareofinformalemploymentbysexandstatusinemploymentfemalenotclassified_iloniflrtste',
                          'shareofinformalemploymentbysexandstatusinemploymentfemaleemployees_iloniflrtste',
                          'worker_output_growth_annual_2015_ilosdgwo', 'womensubjectedtophysicalsexualviolence_cpiawsv',
                          'women_share_of_population_living_with_hiv_15_plus', 'weaponstransfers_globalpi',
                          'wbentp3_wb', 'wbentp2_wb', 'wbentp1_wb', 'wantinglowerimmigrationlevels_mdpild',
                          'used_a_mobile_phone_or_the_internet_to_access_an_account_income_poorest_40_percent_percentage_ages_15_plus',
                          'zoonotic_events_and_the_human_animal_interface', 'unpaiddomesticcare_timeudc',
                          'vulnerablepersonscoveredbysocialassistance_ilospf', 'vulnerability_rank_cw',
                          'working_poverty_total_25', 'voiceandaccountabilityrank_wbva', 'working_poverty_total_15_24',
                          'working_poverty_total_15', 'working_poverty_male_25', 'working_poverty_male_15_24',
                          'violentdemonstation_globalpi', 'working_poverty_male_15',
                          'value_of_exported_goods_as_a_share_of_gdp', 'working_poverty_female_25',
                          'working_poverty_female_15_24', 'unemploymentyouthtotal_cpiauyt', 'working_poverty_female_15',
                          'worker_output_growth_annual_2017_ilosdgwo',
                          'used_a_mobile_phone_or_the_internet_to_access_an_account_percentage_ages_older_25_plus',
                          'used_a_mobile_phone_or_the_internet_to_access_an_account_percentage_ages_15_24',
                          'used_a_mobile_phone_or_the_internet_to_access_an_account_income_richest_60_percent_percentage_ages_15_plus']


async def get_untransformed_indicators():
    async with StorageManager(
            connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
            container_name=os.getenv("AZURE_STORAGE_CONTAINER_NAME"),
            root_folder=os.getenv("ROOT_FOLDER")
    ) as storage_manager:
        print(len(transformed_indicators))
        indicator_cfgs = await storage_manager.get_indicators_cfg()
        untransformed_indicators = []
        for indicator_cfg in indicator_cfgs:
            indicator_id = indicator_cfg['indicator']['indicator_id']
            if indicator_id not in transformed_indicators:
                # source = indicator_cfg['indicator']['source_id']
                # source_cfg = await storage_manager.get_source_cfg(source_id=source)
                # preprocessing_function = source_cfg['source']['preprocessing_function']
                untransformed_indicators.append({
                    'indicator': indicator_cfg['indicator']['indicator_id'],
                    'source': indicator_cfg['indicator']['source_id'],
                    'preprocessing_function': indicator_cfg['indicator']['preprocessing']
                })
        data = pd.DataFrame(untransformed_indicators)
        # drop duplicates in the preprocessing function and keep the first one
        # data.drop_duplicates(subset=['preprocessing_function'], keep='first', inplace=True)
        data.to_csv("untransformed_indicators.csv", index=False)
        return untransformed_indicators


async def list_sources_with_NONE_in_file_format():
    async with StorageManager(
            connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
            container_name=os.getenv("AZURE_STORAGE_CONTAINER_NAME"),
            root_folder=os.getenv("ROOT_FOLDER")
    ) as storage_manager:
        source_cfgs = await storage_manager.get_sources_cfgs()
        sources_with_none = []
        for source_cfg in source_cfgs:
            if source_cfg['source']['file_format'] == "None":
                sources_with_none.append(source_cfg['source']['id'])
        return sources_with_none


async def run_transform_for_vdem_and_hdi_indicators():
    async with StorageManager(
            connection_string=os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
            container_name=os.getenv("AZURE_STORAGE_CONTAINER_NAME"),
            root_folder=os.getenv("ROOT_FOLDER")
    ) as storage_manager:
        print("Start")
        indicator_cfgs = await storage_manager.get_indicators_cfg()
        hdi_and_vdem_indicators = [indicator_cfg['indicator']['indicator_id'] for indicator_cfg in indicator_cfgs if
                                   indicator_cfg['indicator']['source_id'] in ['VDEM', 'HDI']]

        print(hdi_and_vdem_indicators)
        # await transform_sources(indicator_ids=hdi_and_vdem_indicators, project='access_all_data')
        for indicator_id in hdi_and_vdem_indicators:
            indicator_cfg = await storage_manager.get_indicator_cfg(indicator_id=indicator_id)

            await run_transformation_for_indicator(indicator_cfg=indicator_cfg.get('indicator'), project='access_all_data')


if __name__ == "__main__":
    # asyncio.run(upload_source_cfgs(source_cfgs_path=pathlib.Path("/home/thuha/Desktop/Group5/sources")))
    # asyncio.run(upload_indicator_cfgs(
    #     indicator_cfgs_path=pathlib.Path("/home/thuha/Desktop/Group5/indicators")))
    # test_download()
    # asyncio.run(copy_raw_sources())
    # untransformed = asyncio.run(get_untransformed_indicators())
    # print(untransformed)
    # run()
    # sources = asyncio.run(list_sources_with_NONE_in_file_format())
    # print(sources)
    # from dotenv import load_dotenv
    # load_dotenv()
    asyncio.run(run_transform_for_vdem_and_hdi_indicators())
