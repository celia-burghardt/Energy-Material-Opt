#%%
# -*- coding: utf-8 -*-
# SPDX-FileCopyrightText: : 2020-2023 The PyPSA-Eur Authors
#
# SPDX-License-Identifier: MIT
'''
Make new industry_sector_ratios for DE with process heat by temperature level

- space heating and auxiliaries (e.g. lighting) are aggregated for all industrial sectors in df_aggregated
- other sectors are divided by sub-processes in industry_sector_ratios
- for all carriers, final energy demand is taken
- for process heat demand, useful energy is taken
- furnaces heat demand is > 500 °C 
- process heat demand: temperature not known
- in a second step, the process heat demand is divided to temeprature levels with data of Agora industry for temeprature 
ranges per sector for EU27. We assume that the shares per temeprature range per sector is the same for DE as
for EU27. process heat is multiplied by the share for each temeprature range, which is calculated after 
subtracting the furnaces heat demand for some processes which is known to be >500 °C
- some new processes are already added (as in pypsa-eur: recycling for HVC, DRI+EAF for steel), 
more will be added in the next step

assumptions:
- process cooling is electrified
- some processes with electric applications used today are assumed to be fully electrified

'''
import contextlib
import logging
import os
import urllib
from pathlib import Path
import pandas as pd
import pypsa as pypsa

##########################################################################################
# input: which sectors are treated endogenously (default steel, cement, hvc); which country


##########################################################################################
# parameter

index = [ #all fec
    "elec",
    "coal",
    # CB
    "biomass",
    "methane",
    "hydrogen",
    "heat",
    "naphtha",
    "ammonia",
    "process emission",
    "process emission from feedstock",
    "process heat", #ued; in IDEES: all process heat (that is not explicitly furnaces/kilns/high-enthalpy)
    "lowT process heat", #ued; 100 und 100-150, converted from process heat with Agora %
    "mediumT process heat", #ued; 150-200 und 200-500, converted from process heat with Agora %
    "highT process heat", #ued; >500, converted from Agora with %
    "furnaces heat", # ued; in IDEES: all process heat explicitly furnaces/kilns/high-enthalpy
    "production (kt)"] #today. =0 for "new routes, e.g. electrified"

# from _helpers import mute_print
@contextlib.contextmanager
def mute_print():
    with open(os.devnull, "w") as devnull:
        with contextlib.redirect_stdout(devnull):
            yield

# Params for DE
# production in kt (like in IDEES)
params = pd.Series()
params["chlorine_production_today"] = 3179
params["HVC_production_today"] = 13089
# ammonia = pd.read_csv(snakemake.input.ammonia_production, index_col=0)
# ammonia_total = ammonia.loc[ammonia.index.intersection(eu28), str(year)].sum()
params["ammonia_production_today"] = 3125
params["methanol_production_today"] = 1520 #in config in Mt
#params["petrochemical_process_emissions"]
#params["NH3_process_emissions"]
params["MWh_NH3_per_tNH3"] = 5.166
params["MWh_CH4_per_tNH3_SMR"] = 10.8
params["MWh_elec_per_tNH3_SMR"] = 0.7
params["MWh_H2_per_tNH3_electrolysis"] = 6.5
params["MWh_elec_per_tNH3_electrolysis"] =1.17
params["MWh_NH3_per_MWh_H2_cracker"] = 1.46 # https://github.com/euronion/trace/blob/44a5ff8401762edbef80eff9cfe5a47c8d3c8be4/data/efficiencies.csv
params["NH3_process_emissions"] = 24.5 #kt
params["MWh_elec_per_tHVC_mechanical_recycling"] = 0.547
params["MWh_elec_per_tHVC_chemical_recycling"] = 6.9

HVC_primary_fraction: 1.
HVC_mechanical_recycling_fraction: 0.
HVC_chemical_recycling_fraction: 0.
# docs in https://pypsa-eur.readthedocs.io/en/latest/configuration.html#industry

# does not work for the assumed scrap availability in model:  {2045: 0.35, 2050: 0.3}
# 1-(n.stores.loc["DE steel scrap"].e_initial/steel_feedstock.loc["steel scrap", ["H2-DRI+EAF", "EAF"]].sum())/(steel_output*1e3)
# maximum scrap availability for secondary in 2045

St_primary_fraction = {2045: 0.35} 
DRI_fraction = {2020: 0,
    2025: 0,
    2030: 0.05,
    2035: 0.2,
    2040: 0.4,
    2045: 0.7,
    2050: 1}

Al_primary_fraction = {
    2020: 0.4,
    2025: 0.375,
    2030: 0.35,
    2035: 0.325,
    2040: 0.3,
    2045: 0.25,
    2050: 0.2}

params["MWh_H2_per_tCl"] = -0.9372
params["MWh_elec_per_tCl"] = 3.6
params["MWh_elec_per_tMeOH"] = 0.167
params["MWh_CH4_per_tMeOH"] = 10.25
params["H2_DRI"] = 1.7
params["elec_DRI"] = 0.322
# GWh/ktoe OR MWh/toe
toe_to_MWh = 11.630

eu28 = [
    "FR",
    "DE",
    "GB",
    "IT",
    "ES",
    "PL",
    "SE",
    "NL",
    "BE",
    "FI",
    "DK",
    "PT",
    "RO",
    "AT",
    "BG",
    "EE",
    "GR",
    "LV",
    "CZ",
    "HU",
    "IE",
    "SK",
    "LT",
    "HR",
    "LU",
    "SI",
    "CY",
    "MT",
]

sheet_names = {
    "Iron and steel": "ISI",
    "Chemicals Industry": "CHI",
    "Non-metallic mineral products": "NMM",
    "Pulp, paper and printing": "PPA",
    "Food, beverages and tobacco": "FBT",
    "Non Ferrous Metals": "NFM",
    "Transport equipment": "TRE",
    "Machinery equipment": "MAE",
    "Textiles and leather": "TEL",
    "Wood and wood products": "WWP",
    "Other industrial sectors": "OIS",
}
#############################################################################################

def load_idees_data(sector, country = "DE"):
    suffixes = {"out": "", "fec": "_fec", "ued": "_ued", "emi": "_emi"}
    year = 2021
    sheets = {k: sheet_names[sector] + v for k, v in suffixes.items()}
    
    with mute_print():
        idees = pd.read_excel(
            # CB: switch from 2015 -> 2021
            f"../data/raw/JRC-IDEES-2021_Industry_{country}.xlsx",
            sheet_name=list(sheets.values()),
            index_col=0,
            header=0,
        )
    
    for k, v in sheets.items(): 
        idees[k] = idees.pop(v).squeeze()[year]    
    return idees

def iron_and_steel():
    # There are two different approaches to produce iron and steel:
    # i.e., integrated steelworks and electric arc.
    # Electric arc approach has higher efficiency and relies more on electricity.
    # We assume that integrated steelworks will be replaced by electric arc entirely.

    sector = "Iron and steel"
    idees = load_idees_data(sector)
    df = pd.DataFrame(index=index)
    df["aggregated (MWh)"] = 0.0

    ## Electric arc
    sector = "Electric arc" # (incl. electric rolling + el. product finishing)
    df[sector] = 0.0
    s_fec = idees["fec"][52:95]
    assert s_fec.index[0] == sector

    sel = ["Lighting", "Air compressors", "Motor drives", "Fans and pumps"]
    df.at["elec", "aggregated (MWh)"] += s_fec[sel].sum()
    df.at["heat", "aggregated (MWh)"] += s_fec["Low-enthalpy heat"]

    subsector = "Steel: Smelters"
    s_fec = idees["fec"][63:69]
    s_ued = idees["ued"][63:69]
    assert s_fec.index[0] == subsector
    assert s_ued.index[0] == subsector

    # PyPSA: transform all the smelters into methane
    # here: process heat
    key = "Natural gas and biogas"
    eff_met = s_ued[key] / s_fec[key]
    df.at["furnaces heat", sector] += s_ued[subsector] 

    subsector = "Steel: Electric arc"
    s_fec = idees["fec"][69:70]
    assert s_fec.index[0] == subsector

    df.at["elec", sector] += s_fec[subsector]

    subsector = "Steel: Furnaces, refining and rolling"
    # assumed to be 100 % electric 
    s_fec = idees["fec"][70:78]
    s_ued = idees["ued"][70:78]
    assert s_fec.index[0] == subsector
    assert s_ued.index[0] == subsector
    key = "Steel: Furnaces, refining and rolling - Electric"
    eff = s_ued[key] / s_fec[key]
    df.at["elec", sector] += s_ued[subsector] / eff

    subsector = "Steel: Product finishing"
    s_fec = idees["fec"][77:95]
    s_ued = idees["ued"][77:95]
    assert s_fec.index[0] == subsector
    assert s_ued.index[0] == subsector
    key = "Steel: Product finishing - Electric"
    eff = s_ued[key] / s_fec[key]
    df.at["elec", sector] += s_ued[subsector] / eff

    # Process emissions (per physical output)
    s_emi = idees["emi"][52:96]
    assert s_emi.index[0] == sector
    s_out = idees["out"][7:10]
    assert s_out.index[0] == sector

    # tCO2/t material
    df.loc["process emission", sector] += s_emi["Process emissions"] / s_out[sector]

    # final energy consumption MWh/t material
    sources = ["elec", "biomass", "methane", "hydrogen", "heat", "naphtha", "process heat", "furnaces heat"]
    df.loc[sources, sector] = (df.loc[sources, sector] * toe_to_MWh / s_out[sector])
    df.loc["production (kt)", sector] = s_out[sector]
    ## DRI + Electric arc
    # For primary route: DRI with H2 + EAF

    sector = "DRI + Electric arc"
    df[sector] = df["Electric arc"]

    # add H2 consumption for DRI at 1.7 MWh H2 /ton steel
    df.at["hydrogen", sector] = params.loc["H2_DRI"]

    # add electricity consumption in DRI shaft (0.322 MWh/tSl)
    df.at["elec", sector] += params.loc["elec_DRI"]
    df.loc["production (kt)", sector] = 0
    ## Integrated steelworks
    # could be used in combination with CCS)
    # Assume existing fuels are kept, except for furnaces, refining, rolling, finishing -> electricity
    # Ignore 'derived gases' since these are top gases from furnaces
    #-----------------------------------------------------------------------------------------
    sector = "Integrated steelworks"
    df[sector] = 0.0

    s_fec = idees["fec"][3:51]
    assert s_fec.index[0] == sector

    sel = ["Lighting", "Air compressors", "Motor drives", "Fans and pumps"]
    df.loc["elec", "aggregated (MWh)"] += s_fec[sel].sum()
    df.loc["heat", "aggregated (MWh)"] += s_fec["Low-enthalpy heat"]

    subsector = "Steel: Sinter/Pellet-making"
    s_fec = idees["fec"][14:21]
    s_ued = idees["ued"][14:21]
    assert s_fec.index[0] == subsector
    assert s_ued.index[0] == subsector
    df.loc["elec", sector] += s_fec["Electricity"]

    sel = ["Natural gas and biogas", "Fuel oil"]
    df.loc["methane", sector] += s_fec[sel].sum()
    df.loc["coal", sector] += s_fec["Solids"]

    subsector = "Steel: Blast /Basic oxygen furnace"
    s_fec = idees["fec"][20:27]
    s_ued = idees["ued"][20:27]
    assert s_fec.index[0] == subsector
    assert s_ued.index[0] == subsector

    sel = ["Natural gas and biogas", "Fuel oil"]
    df.loc["methane", sector] += s_fec[sel].sum()
    df.loc["coal", sector] += s_fec["Solids"]
    df.loc["coal", sector] += s_fec["Coke"] 
    # add coke to coal

    subsector = "Steel: Furnaces, refining and rolling"
     # assume fully electrified (as already available)
     # added to ISW and EAF since used in all processes
    s_fec = idees["fec"][26:35]
    s_ued = idees["ued"][26:35]
    assert s_fec.index[0] == subsector
    assert s_ued.index[0] == subsector
    key = "Steel: Furnaces, refining and rolling - Electric"
    eff = s_ued[key] / s_fec[key]
    df.loc["elec", sector] += s_ued[subsector] / eff

    subsector = "Steel: Product finishing"
    s_fec = idees["fec"][33:51]
    s_ued = idees["ued"][33:51]
    assert s_fec.index[0] == subsector
    assert s_ued.index[0] == subsector
    key = "Steel: Product finishing - Electric"
    eff = s_ued[key] / s_fec[key]
    # assume fully electrified
    df.loc["elec", sector] += s_ued[subsector] / eff

    # Process emissions (per physical output)

    s_emi = idees["emi"][3:52]
    assert s_emi.index[0] == sector

    s_out = idees["out"][6:9]
    assert s_out.index[0] == sector

    # tCO2/t material
    df.loc["process emission", sector] = s_emi["Process emissions"] / s_out[sector]

    # final energy consumption MWh/t material
    # MWh/t material
    sources = ["elec", "biomass", "methane", "coal", "hydrogen", "heat", "naphtha", "process heat", "furnaces heat"]
    df.loc[sources, sector] = (
        df.loc[sources, sector]
        * toe_to_MWh
        / s_out[sector]
    )

    df.loc[sources, "aggregated (MWh)"] *= toe_to_MWh*1000
    df.loc["production (kt)", sector] = s_out[sector]
    return df

def other_industrial_sectors():
    sector = "Other industrial sectors"

    idees = load_idees_data(sector)

    df = pd.DataFrame(index=index)
    df["aggregated (MWh)"] = 0.0
    df[sector] = 0.0

    s_fec = idees["fec"][3:69]
    s_ued = idees["ued"][3:69]
    assert s_fec.index[0] == sector
    assert s_ued.index[0] == sector

    sel = ["Lighting", "Air compressors", "Motor drives", "Fans and pumps"]
    df.loc["elec", "aggregated (MWh)"] += s_fec[sel].sum()
    df.loc["heat", "aggregated (MWh)"] += s_fec["Low-enthalpy heat"]

    key = "Other Industrial sectors: Electric processing"
    eff_elec = s_ued[key] / s_fec[key]
    df.loc["elec", sector] += s_ued[key] 

    key = "Other Industrial sectors: Thermal processing"
    df.loc["process heat", sector] += s_ued[key] 

    key = "Other Industries: Electric drying"
    eff_elec = s_ued[key] / s_fec[key]
    df.loc["elec", sector] += s_ued["Other Industrial sectors: Drying"] / eff_elec

    key = "Other Industries: Electric cooling"
    eff_elec = s_ued[key] / s_fec[key]
    df.loc["elec", sector] += (
        s_ued["Other Industrial sectors: Process Cooling"] / eff_elec
    )

    # Diesel motors are electrified
    key = "Other Industrial sectors: Diesel motors (incl. biofuels)"
    df.loc["elec", sector] += s_fec[key]
    key = "Other Industrial sectors: Electric machinery"
    df.loc["elec", sector] += s_fec[key]

    # Steam processing
    # eff_biomass = s_ued[15:25]["Biomass"] / s_fec[15:25]["Biomass"]
    df.loc["process heat", sector] += (
        s_ued["Other Industrial sectors: Steam processing"] 
    )

    s_out = idees["out"][3:4]
    assert "Physical output" in str(s_out.index)

    # MWh/t material
    sources = ["elec", "biomass", "methane", "hydrogen", "heat", "naphtha", "process heat", "furnaces heat"]
    df.loc[sources, sector] = (
        df.loc[sources, sector] * toe_to_MWh / s_out["Physical output (index)"])
    df.loc["production (kt)", sector] = s_out["Physical output (index)"]
    df.loc[sources, "aggregated (MWh)"] *= toe_to_MWh*1000

    return df

def wood_and_wood_products():
    sector = "Wood and wood products"
    idees = load_idees_data(sector)

    df = pd.DataFrame(index=index)
    df["aggregated (MWh)"] = 0.0
    df[sector] = 0.0

    s_fec = idees["fec"][3:48]
    s_ued = idees["ued"][3:48]
    assert s_fec.index[0] == sector
    assert s_ued.index[0] == sector

    sel = ["Lighting", "Air compressors", "Motor drives", "Fans and pumps"]
    df.loc["elec", "aggregated (MWh)"] += s_fec[sel].sum()
    df.loc["heat", "aggregated (MWh)"] += s_fec["Low-enthalpy heat"]

    # Efficiency changes due to electrification
    key = "Wood: Electric drying"
    eff_elec = s_ued[key] / s_fec[key]
    df.loc["elec", sector] += s_ued["Wood: Drying"] / eff_elec

    df.loc["elec", sector] += s_fec["Wood: Electric mechanical processes"]
    df.loc["elec", sector] += s_fec["Wood: Finishing Electric"]
    #Wood: Specific processes with steam
    #Wood: Thermal drying
    #Wood: Steam drying
    # Steam processing is supplied with biomass
    # eff_biomass = s_ued[15:25]["Biomass"] / s_fec[15:25]["Biomass"]
    df.loc["process heat", sector] += (
        s_ued["Wood: Specific processes with steam"] 
    )

    s_out = idees["out"][3:4]
    assert "Physical output" in str(s_out.index)

    # MWh/t material
    sources = ["elec", "biomass", "methane", "hydrogen", "heat", "naphtha", "process heat", "furnaces heat"]
    df.loc[sources, sector] = (
        df.loc[sources, sector] * toe_to_MWh / s_out["Physical output (index)"])
    df.loc["production (kt)", sector] = s_out["Physical output (index)"]
    df.loc[sources, "aggregated (MWh)"] *= toe_to_MWh*1000  
    return df


def textiles_and_leather():
    sector = "Textiles and leather"
    idees = load_idees_data(sector)
    df = pd.DataFrame(index=index)
    df["aggregated (MWh)"] = 0.0
    df[sector] = 0.0

    s_fec = idees["fec"][3:59]
    s_ued = idees["ued"][3:59]
    assert s_fec.index[0] == sector
    assert s_ued.index[0] == sector

    sel = ["Lighting", "Air compressors", "Motor drives", "Fans and pumps"]
    df.loc["elec", "aggregated (MWh)"] += s_fec[sel].sum()
    df.loc["heat", "aggregated (MWh)"] += s_fec["Low-enthalpy heat"]

    # Steam processing 
    # eff_biomass = s_ued[15:26]["Biomass and waste"] / s_fec[15:26]["Biomass and waste"]
    df.loc["process heat", sector] += (
        s_ued["Textiles: Pretreatment with steam"] +
        s_ued["Textiles: Wet processing with steam"])

    # Efficiency changes due to electrification
    key = "Textiles: Microwave drying"
    eff_elec = s_ued[key] / s_fec[key]
    df.loc["elec", sector] += s_ued["Textiles: Drying"] / eff_elec

    df.loc["elec", sector] += s_fec["Textiles: Electric general machinery"]
    df.loc["elec", sector] += s_fec["Textiles: Finishing Electric"]

    s_out = idees["out"][3:4]
    assert "Physical output" in str(s_out.index)

    # MWh/t material
    sources = ["elec", "biomass", "methane", "hydrogen", "heat", "naphtha", "process heat", "furnaces heat"]
    df.loc[sources, sector] = (
        df.loc[sources, sector] * toe_to_MWh / s_out["Physical output (index)"])
    df.loc["production (kt)", sector] = s_out["Physical output (index)"]
    df.loc[sources, "aggregated (MWh)"] *= toe_to_MWh*1000

    return df

def transport_equipment():
    sector = "Transport equipment"
    idees = load_idees_data(sector)

    df = pd.DataFrame(index=index)
    df["aggregated (MWh)"] = 0.0
    df[sector] = 0.0

    s_fec = idees["fec"][3:47]
    s_ued = idees["ued"][3:47]
    assert s_fec.index[0] == sector
    assert s_ued.index[0] == sector

    sel = ["Lighting", "Air compressors", "Motor drives", "Fans and pumps"]
    df.loc["elec", "aggregated (MWh)"] += s_fec[sel].sum()
    df.loc["heat", "aggregated (MWh)"] += s_fec["Low-enthalpy heat"]

    # Efficiency changes due to electrification
    key = "Trans. Eq.: Electric Foundries"
    eff_elec = s_ued[key] / s_fec[key]
    df.loc["elec", sector] += s_ued["Trans. Eq.: Foundries"] / eff_elec

    key = "Trans. Eq.: Electric connection"
    eff_elec = s_ued[key] / s_fec[key]
    df.loc["elec", sector] += s_ued["Trans. Eq.: Connection techniques"] / eff_elec

    key = "Trans. Eq.: Heat treatment - Electric"
    eff_elec = s_ued[key] / s_fec[key]
    df.loc["elec", sector] += s_ued["Trans. Eq.: Heat treatment"] / eff_elec

    df.loc["elec", sector] += s_fec["Trans. Eq.: General machinery"]
    df.loc["elec", sector] += s_fec["Trans. Eq.: Product finishing"]

    # Steam processing 
    eff_biomass = s_ued["Biomass and waste"] / s_fec["Biomass and waste"]
    df.loc["process heat", sector] += s_ued["Trans. Eq.: Steam processing"] 

    s_out = idees["out"][3:4]
    assert "Physical output" in str(s_out.index)

    # MWh/t material
    sources = ["elec", "biomass", "methane", "hydrogen", "heat", "naphtha", "process heat", "furnaces heat"]
    df.loc[sources, sector] = (
        df.loc[sources, sector] * toe_to_MWh / s_out["Physical output (index)"])
    df.loc["production (kt)", sector] = s_out["Physical output (index)"]
    df.loc[sources, "aggregated (MWh)"] *= toe_to_MWh*1000
    return df

def machinery_equipment():
    sector = "Machinery equipment"
    idees = load_idees_data(sector)
    df = pd.DataFrame(index=index)
    df["aggregated (MWh)"] = 0.0
    df[sector] = 0.0

    s_fec = idees["fec"][3:47]
    s_ued = idees["ued"][3:47]
    assert s_fec.index[0] == sector
    assert s_ued.index[0] == sector

    sel = ["Lighting", "Air compressors", "Motor drives", "Fans and pumps"]
    df.loc["elec", "aggregated (MWh)"] += s_fec[sel].sum()
    df.loc["heat", "aggregated (MWh)"] += s_fec["Low-enthalpy heat"]

    # Efficiency changes due to electrification
    key = "Mach. Eq.: Electric Foundries"
    eff_elec = s_ued[key] / s_fec[key]
    df.loc["elec", sector] += s_fec["Mach. Eq.: Electric Foundries"] 
    df.loc["process heat", sector] += s_ued["Mach. Eq.: Thermal Foundries"] 

    key = "Mach. Eq.: Electric connection"
    eff_elec = s_ued[key] / s_fec[key]
    df.loc["elec", sector] += s_ued["Mach. Eq.: Connection techniques"] / eff_elec

    key = "Mach. Eq.: Heat treatment - Electric"
    eff_elec = s_ued[key] / s_fec[key]

    df.loc["elec", sector] += s_ued["Mach. Eq.: Heat treatment"] / eff_elec

    df.loc["elec", sector] += s_fec["Mach. Eq.: General machinery"]
    df.loc["elec", sector] += s_fec["Mach. Eq.: Product finishing"]

    # Steam processing 
    eff_biomass = s_ued["Biomass and waste"] / s_fec["Biomass and waste"]
    df.loc["process heat", sector] += s_ued["Mach. Eq.: Steam processing"] 

    s_out = idees["out"][3:4]
    assert "Physical output" in str(s_out.index)

    # MWh/t material
    sources = ["elec", "biomass", "methane", "hydrogen", "heat", "naphtha", "process heat", "furnaces heat"]
    df.loc[sources, sector] = (
        df.loc[sources, sector] * toe_to_MWh / s_out["Physical output (index)"])
    df.loc["production (kt)", sector] = s_out["Physical output (index)"]
    df.loc[sources, "aggregated (MWh)"] *= toe_to_MWh*1000
    return df

def pulp_paper_printing():
    # Pulp, paper and printing can be completely electrified.
    # There are no process emissions associated to this sector.
    # Paper production covers higher temperature processes above 100°C for washing processes  
    sector = "Pulp, paper and printing"
    idees = load_idees_data(sector)

    df = pd.DataFrame(index=index)
    df["aggregated (MWh)"] = 0.0
    # Pulp production

    # Includes three subcategories:
    # (a) Wood preparation, grinding;
    # (b) Pulping; -> biomass or electric
    # (c) Cleaning.

    sector = "Pulp production"

    df[sector] = 0.0

    s_fec = idees["fec"][3:29]
    s_ued = idees["ued"][3:29]
    assert s_fec.index[0] == sector
    assert s_ued.index[0] == sector

    sel = ["Lighting", "Air compressors", "Motor drives", "Fans and pumps"]
    df.loc["elec", "aggregated (MWh)"] += s_fec[sel].sum()

    df.loc["heat",  "aggregated (MWh)"] += s_fec["Low-enthalpy heat"]

    # Industry-specific
    sel = [
        "Pulp: Wood preparation, grinding",
        "Pulp: Cleaning",
        "Pulp: Pulping electric",
    ]
    df.loc["elec", sector] += s_fec[sel].sum()
    eff_bio = s_ued["Biomass and waste"] / s_fec["Biomass and waste"]
    df.loc["process heat", sector] += s_ued["Pulp: Pulping thermal"] 

    s_out = idees["out"][7:12]
    assert sector in str(s_out.index)

    # MWh/t material
    sources = ["elec", "biomass", "methane", "hydrogen", "heat", "naphtha", "process heat", "furnaces heat"]
    df.loc[sources, sector] = (
        df.loc[sources, sector] * toe_to_MWh / s_out["Pulp production (kt)"])
    df.loc["production (kt)", sector] = s_out["Pulp production (kt)"]

    # Paper production

    # Includes three subcategories:
    # (a) Stock preparation;
    # (b) Paper machine;
    # (c) Product finishing.
    #
    # (b) Paper machine and (c) Product finishing are left electric
    # and thermal is moved to biomass. The efficiency is calculated
    # from the pulping process that is already biomass.
    #
    # (a) Stock preparation represents only 7% and its current energy
    # consumption is assumed to be electrified without any change in efficiency.

    sector = "Paper production"

    df[sector] = 0.0

    s_fec = idees["fec"][30:81]
    s_ued = idees["ued"][30:81]
    assert s_fec.index[0] == sector
    assert s_ued.index[0] == sector

    sel = ["Lighting", "Air compressors", "Motor drives", "Fans and pumps"]
    df.loc["elec", "aggregated (MWh)"] += s_fec[sel].sum()
    df.loc["heat", "aggregated (MWh)"] += s_fec["Low-enthalpy heat"]

    # Industry-specific
    df.loc["process heat", sector] += s_ued["Paper: Stock preparation"]

    # add electricity from process that is already electrified
    df.loc["elec", sector] += s_fec["Paper: Paper machine - Electricity"]

    # add electricity from process that is already electrified
    df.loc["elec", sector] += s_fec["Paper: Product finishing - Electricity"]

    s_fec = idees["fec"][55:68]
    s_ued = idees["ued"][55:68]
    assert s_fec.index[0] == "Paper: Paper machine - Steam use"
    assert s_ued.index[0] == "Paper: Paper machine - Steam use"

    eff_bio = s_ued["Biomass and waste"] / s_fec["Biomass and waste"]
    df.loc["process heat", sector] += s_ued["Paper: Paper machine - Steam use"] 

    s_fec = idees["fec"][68:80]
    s_ued = idees["ued"][68:80]
    assert s_fec.index[0] == "Paper: Product finishing - Steam use"
    assert s_ued.index[0] == "Paper: Product finishing - Steam use"

    eff_bio = s_ued["Biomass and waste"] / s_fec["Biomass and waste"]
    df.loc["process heat", sector] += s_ued["Paper: Product finishing - Steam use"] 

    s_out = idees["out"][9:12]
    assert sector in str(s_out.index)

    # MWh/t material
    sources = ["elec", "biomass", "methane", "hydrogen", "heat", "naphtha", "process heat", "furnaces heat"]

    df.loc[sources, sector] = df.loc[sources, sector] * toe_to_MWh / s_out["Paper production  (kt)"]
    df.loc["production (kt)", sector] = s_out["Paper production  (kt)"]

    # Printing and media reproduction

    # (a) Printing and publishing is assumed to be
    # electrified without any change in efficiency.

    sector = "Printing and media reproduction"

    df[sector] = 0.0

    s_fec = idees["fec"][81:94]
    s_ued = idees["ued"][81:94]
    assert s_fec.index[0] == sector
    assert s_ued.index[0] == sector

    sel = ["Lighting", "Air compressors", "Motor drives", "Fans and pumps"]
    df.loc["elec", "aggregated (MWh)"] += s_fec[sel].sum()
    df.loc["heat", "aggregated (MWh)"] += s_fec["Low-enthalpy heat"]

    # Industry-specific
    df.loc["elec", sector] += s_fec["Printing and publishing"]
  
    s_out = idees["out"][9:12]
    assert sector in str(s_out.index)

    # MWh/t material
    sources = ["elec", "biomass", "methane", "hydrogen", "heat", "naphtha", "process heat", "furnaces heat"]
    df.loc[sources, sector] = df.loc[sources, sector] * toe_to_MWh / s_out["Printing and media reproduction (kt paper eq.)"]
    df.loc["production (kt)", sector] = s_out["Printing and media reproduction (kt paper eq.)"]

    df.loc[sources, "aggregated (MWh)"] *= toe_to_MWh*1000
    return df

def food_beverages_tobacco():
        # Food, beverages and tobaco can be completely electrified.
    # There are no process emissions associated to this sector.

    sector = "Food, beverages and tobacco"
    idees = load_idees_data(sector)

    df = pd.DataFrame(index=index)
    df["aggregated (MWh)"] = 0.0
    df[sector] = 0.0

    s_fec = idees["fec"][3:79]
    s_ued = idees["ued"][3:79]
    assert s_fec.index[0] == sector
    assert s_ued.index[0] == sector

    sel = ["Lighting", "Air compressors", "Motor drives", "Fans and pumps"]
    df.loc["elec", "aggregated (MWh)"] += s_fec[sel].sum()

    df.loc["heat", "aggregated (MWh)"] += s_fec["Low-enthalpy heat"]

    # Efficiency changes due to electrification

    key = "Food: Direct Heat - Electric"
    eff_elec = s_ued[key] / s_fec[key]
    df.loc["elec", sector] += s_ued["Food: Oven (direct heat)"] / eff_elec

    key = "Food: Process Heat - Electric"
    eff_elec = s_ued[key] / s_fec[key]
    df.loc["process heat", sector] += s_ued["Food: Specific process heat"] 

    key = "Food: Electric drying"
    eff_elec = s_ued[key] / s_fec[key]
    df.loc["elec", sector] += s_ued["Food: Drying"] / eff_elec 

    key = "Food: Electric cooling"
    eff_elec = s_ued[key] / s_fec[key]
    df.loc["elec", sector] += (
        s_ued["Food: Process cooling and refrigeration"] / eff_elec
    )

    # in pypsa-eur: biomass
    df.loc["process heat", sector] += s_ued["Food: Steam processing"]

    # add electricity from process that is already electrified
    df.loc["elec", sector] += s_fec["Food: Electric machinery"]

    s_out = idees["out"][3:4]
    assert "Physical output (index)" in str(s_out.index)

    # MWh/t material
    sources = ["elec", "biomass", "methane", "hydrogen", "heat", "naphtha", "process heat", "furnaces heat"]
    df.loc[sources, "aggregated (MWh)"] *= toe_to_MWh*1000
    df.loc[sources, sector] = (
        df.loc[sources, sector] * toe_to_MWh / s_out["Physical output (index)"]
    )
    df.loc["production (kt)", sector] = s_out["Physical output (index)"]

    return df

def non_ferrous_metals():
    sector = "Non Ferrous Metals"
    idees = load_idees_data(sector)

    df = pd.DataFrame(index=index)

    # Alumina

    # In Pypsa-eur: High-enthalpy heat is converted to methane, refining electrified.
    # Process heat at T>500C is required here.
    # There are no process emissions associated to Alumina manufacturing.
    df["aggregated (MWh)"] = 0.0
    sector = "Alumina production"

    df[sector] = 0.0

    s_fec = idees["fec"][3:31]
    s_ued = idees["ued"][3:31]
    assert s_fec.index[0] == sector
    assert s_ued.index[0] == sector

    sel = ["Lighting", "Air compressors", "Motor drives", "Fans and pumps"]
    df.loc["elec", "aggregated (MWh)"] += s_fec[sel].sum()

    df.loc["heat", "aggregated (MWh)"] += s_fec["Low-enthalpy heat"]

    s_fec = idees["fec"][14:25]
    s_ued = idees["ued"][14:25]
    assert s_fec.index[0] == "Alumina production: High-enthalpy heat"
    assert s_ued.index[0] == "Alumina production: High-enthalpy heat"

    df.loc["furnaces heat", sector] += (
        s_ued["Alumina production: High-enthalpy heat"]
    )

    s_fec = idees["fec"][25:31]
    s_ued = idees["ued"][25:31]
    assert s_fec.index[0] == "Alumina production: Refining"
    assert s_ued.index[0] == "Alumina production: Refining"

    df.loc["process heat", sector] += s_ued["Alumina production: Refining"] 

    s_out = idees["out"][9:10]
    assert sector in str(s_out.index)

    # MWh/t material
    sources = ["elec", "biomass", "methane", "hydrogen", "heat", "naphtha", "process heat", "furnaces heat"]
    df.loc[sources, sector] = (
        df.loc[sources, sector] * toe_to_MWh / s_out["Alumina production (kt)"]
    )
    df.loc["production (kt)", sector] = s_out["Alumina production (kt)"]

    # Aluminium primary route
    sector = "Aluminium - primary production"

    df[sector] = 0.0

    s_fec = idees["fec"][32:68]
    s_ued = idees["ued"][32:68]
    assert s_fec.index[0] == sector
    assert s_ued.index[0] == sector

    sel = ["Lighting", "Air compressors", "Motor drives", "Fans and pumps"]
    df.loc["elec", "aggregated (MWh)"] += s_fec[sel].sum()

    df.loc["heat", "aggregated (MWh)"] += s_fec["Low-enthalpy heat"]

    # Add aluminium  electrolysis (smelting)
    df.loc["elec", sector] += s_fec["Aluminium electrolysis (smelting)"]

    # Efficiency changes due to electrification
    key = "Aluminium processing - Electric"
    eff_elec = s_ued[key] / s_fec[key]

    key = "Aluminium processing  (metallurgy e.g. cast house, reheating)"
    df.loc["process heat", sector] += s_ued[key] 

    key = "Aluminium finishing - Electric"
    eff_elec = s_ued[key] / s_fec[key]
    df.loc["process heat", sector] += s_ued["Aluminium finishing"] 

    s_emi = idees["emi"][32:70]
    assert s_emi.index[0] == sector

    s_out = idees["out"][10:13]
    assert sector in str(s_out.index)
    df.loc["production (kt)", sector] = s_out["Aluminium - primary production"]
    # tCO2/t material
    df.loc["process emission", sector] = (
        s_emi["Process emissions"] / s_out["Aluminium - primary production"]
    )

    # MWh/t material
    sources = ["elec", "biomass", "methane", "hydrogen", "heat", "naphtha", "process heat", "furnaces heat"]
    df.loc[sources, sector] = (
        df.loc[sources, sector] * toe_to_MWh / s_out["Aluminium - primary production"]
    )

    # Aluminium secondary route

    sector = "Aluminium - secondary production"
    df[sector] = 0.0

    s_fec = idees["fec"][70:112]
    s_ued = idees["ued"][70:112]
    assert s_fec.index[0] == sector
    assert s_ued.index[0] == sector

    sel = ["Lighting", "Air compressors", "Motor drives", "Fans and pumps"]
    df.loc["elec", "aggregated (MWh)"] += s_fec[sel].sum()

    df.loc["heat", "aggregated (MWh)"] += s_fec["Low-enthalpy heat"]

    key = "Secondary aluminium - Electric"
    eff_elec = s_ued[key] / s_fec[key]
    key = "Secondary aluminium (incl. pre-treatment, remelting)"
    df.loc["process heat", sector] += s_ued[key] 

    key = "Aluminium processing - Electric"
    eff_elec = s_ued[key] / s_fec[key]
    key = "Aluminium processing  (metallurgy e.g. cast house, reheating)"
    df.loc["process heat", sector] += s_ued[key]

    key = "Aluminium finishing - Electric"
    eff_elec = s_ued[key] / s_fec[key]
    df.loc["process heat", sector] += s_ued["Aluminium finishing"]

    s_out = idees["out"][10:14]
    assert sector in str(s_out.index)
    df.loc["production (kt)", sector] = s_out["Aluminium - secondary production"]

    # MWh/t material
    sources = ["elec", "biomass", "methane", "hydrogen", "heat", "naphtha", "process heat", "furnaces heat"]
    df.loc[sources, sector] = (
        df.loc[sources, sector] * toe_to_MWh / s_out["Aluminium - secondary production"]
    )

    # electrified secondary production
    sector = "Aluminium - secondary production - electric"
    df[sector] = 0.0

    key = "Secondary aluminium - Electric"
    eff_elec = s_ued[key] / s_fec[key]
    key = "Secondary aluminium (incl. pre-treatment, remelting)"
    df.loc["elec", sector] += s_ued[key] / eff_elec

    key = "Aluminium processing - Electric"
    eff_elec = s_ued[key] / s_fec[key]
    key = "Aluminium processing  (metallurgy e.g. cast house, reheating)"
    df.loc["elec", sector] += s_ued[key]/eff_elec

    key = "Aluminium finishing - Electric"
    eff_elec = s_ued[key] / s_fec[key]
    df.loc["elec", sector] += s_ued["Aluminium finishing"]/eff_elec

    s_out = idees["out"][10:14]
    
    df.loc["production (kt)", sector] = 0

    # MWh/t material
    sources = ["elec", "biomass", "methane", "hydrogen", "heat", "naphtha", "process heat", "furnaces heat"]
    df.loc[sources, sector] = (
        df.loc[sources, sector] * toe_to_MWh / s_out["Aluminium - secondary production"]
    )

    # Other non-ferrous metals

    sector = "Other non-ferrous metals"
    df[sector] = 0.0
    s_fec = idees["fec"][113:157]
    s_ued = idees["ued"][113:157]
    assert s_fec.index[0] == sector
    assert s_ued.index[0] == sector

    sel = ["Lighting", "Air compressors", "Motor drives", "Fans and pumps"]
    df.loc["elec", "aggregated (MWh)"] += s_fec[sel].sum()

    df.loc["heat", "aggregated (MWh)"] += s_fec["Low-enthalpy heat"]

    key = "Metal production - Electric"
    eff_elec = s_ued[key] / s_fec[key]
    df.loc["process heat", sector] += s_ued["Other Metals: production"]

    key = "Metal processing - Electric"
    eff_elec = s_ued[key] / s_fec[key]
    key = "Metal processing  (metallurgy e.g. cast house, reheating)"
    df.loc["process heat", sector] += s_ued[key] 

    key = "Metal finishing - Electric"
    eff_elec = s_ued[key] / s_fec[key]
    df.loc["process heat", sector] += s_ued["Metal finishing"] 

    s_emi = idees["emi"][113:157]
    assert s_emi.index[0] == sector

    s_out = idees["out"][13:14]
    assert sector in str(s_out.index)
    df.loc["production (kt)", sector] = s_out["Other non-ferrous metals (kt lead eq.)"]

    # tCO2/t material
    df.loc["process emission", sector] = (
        s_emi["Process emissions"] / s_out["Other non-ferrous metals (kt lead eq.)"]
    )

    # MWh/t material
    sources = ["elec", "biomass", "methane", "hydrogen", "heat", "naphtha", "process heat", "furnaces heat"]
    df.loc[sources, sector] = (
        df.loc[sources, sector]
        * toe_to_MWh
        / s_out["Other non-ferrous metals (kt lead eq.)"]
    )

    df.loc[sources, "aggregated (MWh)"] *= toe_to_MWh*1000
    return df

def nonmetalic_mineral_products():
    # This includes cement, ceramic and glass production.
    # This includes process emissions related to the fabrication of clinker.

    sector = "Non-metallic mineral products"
    idees = load_idees_data(sector)

    df = pd.DataFrame(index=index)
    df["aggregated (MWh)"] = 0.0

    # Cement

    # This sector has process-emissions.
    # Includes three subcategories:
    # (a) Grinding, milling of raw material,
    # (b) Pre-heating and pre-calcination,
    # (c) clinker production (kilns),
    # (d) Grinding, packaging.
    # (b)+(c) represent 94% of fec. So (a) is joined to (b) and (d) is joined to (c).
    # Temperatures above 1400C are required for processing limestone and sand into clinker.
    # Everything (except current electricity and heat consumption and existing biomass)
    # is transformed into methane for high T.

    sector = "Cement"
    df[sector] = 0.0

    s_fec = idees["fec"][3:25]
    s_ued = idees["ued"][3:25]
    assert s_fec.index[0] == sector
    assert s_ued.index[0] == sector

    sel = ["Lighting", "Air compressors", "Motor drives", "Fans and pumps"]
    df.loc["elec", "aggregated (MWh)"] += s_fec[sel].sum()
    df.loc["heat", "aggregated (MWh)"] += s_fec["Low-enthalpy heat"]

    # pre-processing: keep existing elec and biomass, rest to methane
    df.loc["elec", sector] += s_fec["Cement: Grinding, milling of raw material"]
    df.loc["furnaces heat", sector] += s_ued["Cement: Pre-heating and pre-calcination"]
    
    # furnaces
    subsector = "Cement: Clinker production (kilns)"
    s_fec = idees["fec"][23:33]
    s_ued = idees["ued"][23:33]
    s_emi = idees["emi"][23:33]
    assert s_fec.index[0] == subsector
    assert s_ued.index[0] == subsector
    assert s_emi.index[0] == subsector
    df.loc["furnaces heat", sector] += s_ued["Cement: Clinker production (kilns)"] 
    eff_coal_furnace = s_ued["Solids"] / s_fec["Solids"] 
    eff_waste_furnace = s_ued["Biomass and waste"] / s_fec["Biomass and waste"] 
    EF_coal_furnace = s_emi["Solids"] / s_ued["Solids"]
    EF_waste_furnace = s_emi["Biomass and waste"] / s_ued["Biomass and waste"]

    # grinding
    s_fec = idees["fec"][31:35]
    df.loc["elec", sector] += s_fec["Cement: Grinding, packaging and precasting (electricity)"]
    df.loc["process heat", sector] += s_fec["Cement: Precasting - Steam"]

    # Process emissions
    # come from calcination of limestone to chemically reactive calcium oxide (lime).
    # Calcium carbonate -> lime + CO2
    # CaCO3  -> CaO + CO2
    s_emi = idees["emi"][3:45]
    assert s_emi.index[0] == sector

    s_out = idees["out"][7:8]
    assert sector in str(s_out.index)

    # tCO2/t material
    df.loc["process emission", sector] += s_emi["Process emissions"] / s_out["Cement (kt)"]

    # MWh/t material
    sources = ["elec", "biomass", "methane", "hydrogen", "heat", "naphtha", "coal", "process heat", "furnaces heat"]
    df.loc[sources, sector] = (df.loc[sources, sector] * toe_to_MWh / s_out["Cement (kt)"]
    )
    df.loc["production (kt)", sector] = s_out["Cement (kt)"]

    # Ceramics & other NMM
    # This sector has process emissions.
    # Includes four subcategories:
    # (a) Mixing of raw material,
    # (b) Drying and sintering of raw material,
    # (c) Primary production process,
    # (d) Product finishing.
    # (b) represents 65% of fec and (a) 4%. So (a) is joined to (b).
    # Everything is electrified

    sector = "Ceramics & other NMM"
    df[sector] = 0.0

    s_fec = idees["fec"][46:96]
    s_ued = idees["ued"][46:96]
    assert s_fec.index[0] == sector
    assert s_ued.index[0] == sector

    sel = ["Lighting", "Air compressors", "Motor drives", "Fans and pumps"]
    df.loc["elec", "aggregated (MWh)"] += s_fec[sel].sum()
    df.loc["heat", "aggregated (MWh)"] += s_fec["Low-enthalpy heat"]

    df.loc["elec", sector] += s_fec["Ceramics: Mixing of raw material"]

    df.loc["process heat", sector] += s_fec["Ceramics: Drying and sintering of raw material"]

    key = "Ceramics: Electric kiln"
    eff_elec = s_ued[key] / s_fec[key]

    df.loc["elec", sector] += s_ued["Ceramics: Electric kiln"]
    df.loc["process heat", sector] += s_ued["Ceramics: Thermal kiln"]

    key = "Ceramics: Electric furnace"
    eff_elec = s_ued[key] / s_fec[key]
    df.loc["elec", sector] += s_ued[key]

    key = "Ceramics: Thermal furnace"
    df.loc["process heat", sector] += s_ued[key]

    s_emi = idees["emi"][46:97]
    assert s_emi.index[0] == sector

    s_out = idees["out"][8:9]
    assert sector in str(s_out.index)

    # tCO2/t material
    df.loc["process emission", sector] += s_emi["Process emissions"] / s_out.values

    # MWh/t material
    sources = ["elec", "biomass", "methane", "hydrogen", "heat", "naphtha", "process heat", "furnaces heat"]

    df.loc[sources, sector] = df.loc[sources, sector] * toe_to_MWh / s_out["Ceramics & other NMM (kt bricks eq.)"]
    df.loc["production (kt)", sector] = s_out["Ceramics & other NMM (kt bricks eq.)"]

    # Glass production
    # This sector has process emissions.
    # Includes four subcategories:
    # (a) Melting tank
    # (b) Forming
    # (c) Annealing
    # (d) Finishing processes.
    # (a) represents 73%. (b), (d) are joined to (c).
    # Everything is electrified.

    sector = "Glass production"

    df[sector] = 0.0

    s_fec = idees["fec"][97:127]
    s_ued = idees["ued"][97:127]
    assert s_fec.index[0] == sector
    assert s_ued.index[0] == sector

    sel = ["Lighting", "Air compressors", "Motor drives", "Fans and pumps"]
    df.loc["elec", "aggregated (MWh)"] += s_fec[sel].sum()
    df.loc["heat", "aggregated (MWh)"] += s_fec["Low-enthalpy heat"]

    # Efficiency changes due to electrification
    key = "Glass: Electric melting tank"
    eff_elec = s_ued[key] / s_fec[key]
    df.loc["elec", sector] += s_ued[key] 

    key = "Glass: Thermal melting tank"
    df.loc["furnaces heat", sector] += s_ued[key]

    key = "Glass: Annealing - electric"
    eff_elec = s_ued[key] / s_fec[key]

    sel = ["Glass: Forming", "Glass: Annealing", "Glass: Finishing processes"]
    df.loc["elec", sector] += s_ued[sel].sum() / eff_elec

    s_emi = idees["emi"][97:128]
    assert s_emi.index[0] == sector

    s_out = idees["out"][9:10]
    assert sector in str(s_out.index)

    # tCO2/t material
    df.loc["process emission", sector] += s_emi["Process emissions"] / s_out["Glass production  (kt)"]

    # MWh/t material
    sources = ["elec", "biomass", "methane", "hydrogen", "heat", "naphtha", "process heat", "furnaces heat"]
    df.loc[sources, sector] = (df.loc[sources, sector] * toe_to_MWh
        / s_out["Glass production  (kt)"]
    )
    df.loc["production (kt)", sector] = s_out["Glass production  (kt)"]

    df.loc[sources, "aggregated (MWh)"] *= toe_to_MWh*1000
    return df


#%%

def basic_chemicals():
    sector = "Chemicals Industry"
    idees = load_idees_data(sector)
    df = pd.DataFrame(index=index)
    df_eff = pd.DataFrame(index=index)
    
    # Basic chemicals
    sector = "Basic chemicals"
    name = "aggregated (MWh)"
    df[name] = 0.0
    s_fec = idees["fec"][3:9]
    s_ued = idees["ued"][3:9]
    assert s_fec.index[0] == sector
    assert s_ued.index[0] == sector
    sel = ["Lighting", "Air compressors", "Motor drives", "Fans and pumps"]
    df.loc["elec", name] += s_fec[sel].sum()
    df.loc["heat", name] += s_fec["Low-enthalpy heat"] 
    
    # steam
    subsector = "Chemicals: Steam processing"
    name = "Basic chemicals"
    df[name] = 0.0
    # All the final energy consumption in the steam processing is
    # converted to methane, since we need >1000 C temperatures here.
    # The current efficiency of methane is assumed in the conversion.
    # CB: ??? I did not find > 1000 C in literature except for steamcracking which should 
    # be in "furnaces heat"
    s_fec = idees["fec"][23:34]
    s_ued = idees["ued"][23:34]
    assert s_fec.index[0] == subsector
    assert s_ued.index[0] == subsector
    df_eff.loc["methane", "steam"]  = s_ued["Natural gas and biogas"] / s_fec["Natural gas and biogas"]
    df_eff.loc["biomass", "steam"]  = s_ued["Biomass and waste"] / s_fec["Biomass and waste"]
    df.loc["process heat", name] += s_ued[subsector] 

    # process cooling
    subsector = "Chemicals: Process cooling"
    s_fec = idees["fec"][42:56]
    s_ued = idees["ued"][42:56]
    assert s_fec.index[0] == subsector
    assert s_ued.index[0] == subsector
    key = "Chemicals: Process cooling - Electric"
    eff_elec = s_ued[key] / s_fec[key]
    # assume fully electrified
    df.loc["elec", name] += s_ued[subsector] / eff_elec

    # feedstock
    subsector = "Chemicals: Feedstock (energy used as raw material)"
    # There are Solids, Refinery gas, LPG, Diesel oil, Residual fuel oil,
    # Other liquids, Naphtha, Natural gas for feedstock.
    # Naphta represents 47%, methane 17%. LPG (18%) solids, refinery gas,
    # diesel oil, residual fuel oils and other liquids are asimilated to Naphtha
    s_fec = idees["fec"][14:23] 
    assert s_fec.index[0] == subsector
    df.loc["methane", name] += s_fec["Natural gas"]
       
       
    # TODO: now all assigned to methane
    sel = [ "Solids",
        "Refinery gas",       
        "LPG",
        "Diesel oil",
        "Fuel oil",]
    df.loc["methane", name] += s_fec[sel].sum()

    df.loc["naphtha", name] += s_fec[["Naphtha", "Other liquids"]].sum() 
    # furnaces
    subsector = "Chemicals: Furnaces"
    s_fec = idees["fec"][34:42]
    s_ued = idees["ued"][34:42]
    assert s_fec.index[0] == subsector
    assert s_ued.index[0] == subsector
    #efficiencies
    # assumption: all to methane
    subsector = "Chemicals: Furnaces - Thermal"
    key = "Diesel oil and liquid biofuels"
    df_eff.loc["naphtha", name] = s_ued[key] / s_fec[key]
    df.loc["naphtha", name] += s_fec[key]
    key = "Fuel oil"
    df.loc["naphtha", name] += s_fec[key]
    key = "Solids"
    df_eff.loc["coal", name] = s_ued[key] / s_fec[key]
    df.loc["coal", name] += s_fec[key]
    key = "Natural gas and biogas"
    df_eff.loc["methane", name] = s_ued[key] / s_fec[key]
    df.loc["methane", name] += s_fec[key]
    
    key = "Chemicals: Furnaces - Electric"
    df_eff.loc["elec", name] = s_ued[key] / s_fec[key]
    df.loc["elec", name] += s_fec[key]

    # electric processes
    subsector = "Chemicals: Generic electric process"
    s_fec = idees["fec"][56:57]
    assert s_fec.index[0] == subsector
    df.loc["elec", name] += s_fec[subsector]

    s_emi = idees["emi"][3:59]
    assert s_emi.index[0] == sector

    s_out = idees["out"][8:9]
    assert sector in str(s_out.index)

    # seperate df_totals and df
    df_totals = df.copy()

    # MWh/t material
    sources = ["elec", "biomass", "methane", "hydrogen", "heat", "naphtha", "coal", "process heat", "furnaces heat"]
    df.loc[sources, sector] = (df.loc[sources, sector] * toe_to_MWh
        / s_out["Basic chemicals (kt ethylene eq.)"])
    df.loc["production (kt)", sector] = s_out["Basic chemicals (kt ethylene eq.)"]
    df_totals.loc[sources, sector] = (df_totals.loc[sources, sector] * toe_to_MWh*1000)
    
    # tCO2/t material
    df.loc["process emission", sector] += s_emi["Process emissions"] / s_out["Basic chemicals (kt ethylene eq.)"]
    df_totals.loc["process emission", sector] += s_emi["Process emissions"]*1000
   
    # distribute to products
    # ammonia
    name = sector + " | NH3 + SMR"
    df[name] = 0
    df_totals[name] = 0
    ammonia_total = params["ammonia_production_today"]
    df.loc["methane", name] = params["MWh_CH4_per_tNH3_SMR"]
    df.loc["elec", name] = params["MWh_elec_per_tNH3_SMR"]
    df.loc["process emission", name] = params["NH3_process_emissions"]/ammonia_total #kt/kt
    df_totals[name] = df[name]*ammonia_total*1e3
    df.loc["production (kt)", name] = ammonia_total
    
    # methanol
    name = sector + " | methanol"
    df[name] = 0
    methanol_total = params["methanol_production_today"]
    df.loc["methane", name] = params["MWh_CH4_per_tMeOH"]
    df.loc["elec", name] = params["MWh_elec_per_tMeOH"]
    df.loc["process emission", name] = 0 # process emissions are in feedstock   
    df_totals[name] = df[name]*methanol_total*1e3
    df.loc["production (kt)", name] = methanol_total

    name = "Basic chemicals | Cl2"   
    df[name] = 0
    df_totals[name] = 0
    s_fec = idees["fec"][56:57]
    assert s_fec.index[0] == subsector
    df.loc["elec", name] = params["MWh_elec_per_tCl"]
    df_totals[name] = df[name]*params["chlorine_production_today"]*1e3
    df.loc["production (kt)", name] = params["chlorine_production_today"]
    # df.loc["elec", name] = params["MWh_elec_per_tCl"] #cb: with this approach, electricity balance 
    # does not fit with IDEES database and an electricity demand of 11.4 TWh for Cl2 production while 
    # in literature, 9.6 TWh is reported for 2017 (p.28, Roadmap Chemie 2050 of 2019). 
    #df_totals[name] = df[name]*chlorine_total
    #df_totals.loc["elec", name] = df_totals.loc["elec", "Basic chemicals | processes | electric"] 
    
    df_totals[sector + " | HVC"] = df_totals[sector] - df_totals[[sector + " | methanol", sector + " | Cl2", sector + " | NH3 + SMR"]].sum(axis=1)
    df[sector + " | HVC"] = df_totals[sector + " | HVC"]/(params["HVC_production_today"]*1e3)
    df.loc["production (kt)", sector + " | HVC"] = (params["HVC_production_today"])
   
    ############################################################
    # 3. add "future" processes: HVC recycling, endogenous H2 for ammonia production,   
    # b) adjust: add H2 byproduction of chlor-alkali
    # c) add embedded emissions in HVC
    #############################################################
    # HVC mechanical recycling
    name = sector + " | HVC mechanical recycling"
    df[name] = 0.0
    df.loc["elec", name] = params["MWh_elec_per_tHVC_mechanical_recycling"]
    # df_totals[name] = df[name]*1.98*1e6
    # https://www.umweltbundesamt.de/daten/ressourcen-abfall/verwertung-entsorgung-ausgewaehlter-abfallarten/kunststoffabfaelle#hohe-verwertungsquoten-

    # HVC chemical recycling
    name = sector + " | HVC chemical recycling"
    df[name] = 0.0
    df.loc["elec", name] = params["MWh_elec_per_tHVC_chemical_recycling"]

    # H2 byproduction Cl2
    name = sector + " | Cl2"
    df.loc["hydrogen", name] = params["MWh_H2_per_tCl"]

    # split NH3 into SMR and Haber-Bosch synthesis
    name = sector + " | NH3"
    df[name] = 0.0
    df.loc["hydrogen", name] = params["MWh_H2_per_tNH3_electrolysis"]
    df.loc["elec", name] = params["MWh_elec_per_tNH3_electrolysis"]
    df.loc["methane", name] = 0
    df.loc["process emission", name] = 0
    df.loc["production (kt)", name] = ammonia_total
    # methanolisation: in building up of energy system

    # emissions originating from feedstock, could be non-fossil origin
    # additional, since not accounted for in emissions accounting (Kullmann 2023)
    # (TODO: check in description of IDEES)
    # oil EF * naphtha content
    # tCO2/t material
    name = sector + " | HVC"
    df.loc["process emission from feedstock", name] += 0.25*df.loc["naphtha", name]
    return df, df_totals

def other_chemicals():
    # Other chemicals
    idees = load_idees_data("Chemicals Industry")
    df = pd.DataFrame(index=index)
    sector = "Other chemicals"
    name = "Other chemicals"
    name = "aggregated (MWh)"
    df[name] = 0.0
    s_fec = idees["fec"][59:67]
    s_ued = idees["ued"][59:67]
    assert s_fec.index[0] == sector
    sel = ["Lighting", "Air compressors", "Motor drives", "Fans and pumps"]
    df.loc["elec", name] += s_fec[sel].sum()
    df.loc["heat", name] += s_ued["Low-enthalpy heat"] 

    df[sector] = 0.0
    subsector = "High-enthalpy heat processing - Steam"
    s_fec = idees["fec"][71:84]
    s_ued = idees["ued"][71:84]
    assert s_fec.index[0] == subsector
    assert s_ued.index[0] == subsector
    df.loc["process heat", sector] += s_ued[subsector]
    
    key = "High-enthalpy heat processing - Electric (microwave)"
    eff_elec = s_ued[key] / s_fec[key]
    df.loc["elec", sector] += s_fec[key] 
   
    subsector = "Chemicals: Furnaces"
    s_fec = idees["fec"][83:91]
    s_ued = idees["ued"][83:91]
    assert s_fec.index[0] == subsector
    assert s_ued.index[0] == subsector
    key = "Chemicals: Furnaces - Thermal"
    df.loc["furnaces heat", sector] += s_ued[key] 
    key = "Chemicals: Furnaces - Electric"
    eff_elec = s_fec[key] / s_fec[key]
    df.loc["elec", sector] += s_fec[key] 
        
    # assume cooling fully electrified
    subsector = "Chemicals: Process cooling"
    s_fec = idees["fec"][91:105]
    s_ued = idees["ued"][91:105]
    assert s_fec.index[0] == subsector
    assert s_ued.index[0] == subsector
    key = "Chemicals: Process cooling - Electric"
    eff = s_ued[key] / s_fec[key]
    df.loc["elec", sector] += s_ued[subsector] / eff

    subsector = "Chemicals: Generic electric process"
    s_fec = idees["fec"][105:106]
    assert s_fec.index[0] == subsector
    df.loc["elec", sector] += s_fec[subsector]

    # Process emissions
    s_emi = idees["emi"][59:108]
    s_out = idees["out"][9:10]
    assert s_emi.index[0] == sector
    assert sector in str(s_out.index)

    # tCO2/t
    df.loc["process emission", sector] += s_emi["Process emissions"]/s_out["Other chemicals (kt ethylene eq.)"]
    
    sources = ["elec", "biomass", "methane", "hydrogen", "heat", "naphtha", "coal", "process heat", "furnaces heat"]

    df.loc[sources, sector] = df.loc[sources, sector] * toe_to_MWh/s_out["Other chemicals (kt ethylene eq.)"]
    df.loc["production (kt)", sector] = s_out["Other chemicals (kt ethylene eq.)"]

    # Pharmaceutical products #######################################################
    sector = "Pharmaceutical products etc."
    name = "Pharmaceutical products etc."
    df[sector] = 0.0
    s_fec = idees["fec"][108:114]
    s_ued = idees["ued"][108:114]
    assert s_fec.index[0] == sector
    sel = ["Lighting", "Air compressors", "Motor drives", "Fans and pumps"]
    df.loc["elec", "aggregated (MWh)"] += s_fec[sel].sum()
    df.loc["heat", "aggregated (MWh)"] += s_ued["Low-enthalpy heat"]
  
    subsector = "High-enthalpy heat processing - Steam"
    s_fec = idees["fec"][120:132]
    s_ued = idees["ued"][120:132]
    assert s_fec.index[0] == subsector
    assert s_ued.index[0] == subsector
    df.loc["process heat", sector] += s_ued[subsector] 
    
    key = "High-enthalpy heat processing - Electric (microwave)"
    eff_elec = s_ued[key] / s_fec[key]
    df.loc["elec", sector] += s_fec[key] 

    subsector = "Chemicals: Furnaces"
    s_fec = idees["fec"][132:140]
    s_ued = idees["ued"][132:140]
    assert s_fec.index[0] == subsector
    assert s_ued.index[0] == subsector
    key = "Chemicals: Furnaces - Thermal"
    df.loc["furnaces heat", sector] += s_ued[key] 
    key = "Chemicals: Furnaces - Electric"
    eff_elec = s_fec[key] / s_fec[key]
    df.loc["elec", sector] += s_fec[key] 

    subsector = "Chemicals: Process cooling"
    s_fec = idees["fec"][140:156]
    s_ued = idees["ued"][140:156]
    assert s_fec.index[0] == subsector
    assert s_ued.index[0] == subsector
    key = "Chemicals: Process cooling - Electric"
    eff_elec = s_ued[key] / s_fec[key]
    # assume fully electrified
    df.loc["elec", sector] += s_ued[subsector] / eff_elec

    subsector = "Chemicals: Generic electric process"
    s_fec = idees["fec"][154:155]
    s_out = idees["out"][10:11]
    assert s_fec.index[0] == subsector
    assert sector in str(s_out.index)
    df.loc["elec", sector] += s_fec[subsector]

    # tCO2/t
    df.loc["process emission", sector] += 0.0

    # MWh/t material
    sources = ["elec", "biomass", "methane", "hydrogen", "heat", "naphtha", "coal", "process heat", "furnaces heat"]

    df.loc[sources, sector] = df.loc[sources, sector] * toe_to_MWh/s_out["Pharmaceutical products etc. (kt ethylene eq.)"]
    df.loc["prodution (kt)", sector] = s_out["Pharmaceutical products etc. (kt ethylene eq.)"]
    return df

def cement_highT_furnace_data():
    sector = "Non-metallic mineral products"
    idees = load_idees_data(sector)
    subsector = "Cement: Clinker production (kilns)"
    s_fec = idees["fec"][23:33]
    s_ued = idees["ued"][23:33]
    s_emi = idees["emi"][23:33]
    assert s_fec.index[0] == subsector
    assert s_ued.index[0] == subsector
    assert s_emi.index[0] == subsector
    
    eff_coal_furnace = s_ued["Solids"] / s_fec["Solids"] 
    eff_waste_furnace = s_ued["Biomass and waste"] / s_fec["Biomass and waste"] 
    EF_coal_furnace = s_emi["Solids"] / s_fec["Solids"]
    EF_waste_furnace = s_emi["Biomass and waste"] / s_fec["Biomass and waste"]
    return eff_coal_furnace, eff_waste_furnace, EF_coal_furnace, EF_waste_furnace

def new_industry_sector_ratios():
    df_aggregated = pd.Series(index = index, data = 0)
    industry_sector_ratios = other_industrial_sectors()
    df_aggregated += industry_sector_ratios["aggregated (MWh)"] 

    df = wood_and_wood_products()
    industry_sector_ratios[df.columns] = df
    df_aggregated += df["aggregated (MWh)"]

    df = textiles_and_leather()
    industry_sector_ratios[df.columns] = df
    df_aggregated += df["aggregated (MWh)"]

    df = transport_equipment()
    industry_sector_ratios[df.columns] = df
    df_aggregated += df["aggregated (MWh)"]

    df = machinery_equipment()
    industry_sector_ratios[df.columns] = df
    df_aggregated += df["aggregated (MWh)"]

    df = pulp_paper_printing()
    industry_sector_ratios[df.columns] = df
    df_aggregated += df["aggregated (MWh)"]

    df = food_beverages_tobacco()
    industry_sector_ratios[df.columns] = df
    df_aggregated += df["aggregated (MWh)"]

    df = non_ferrous_metals()
    industry_sector_ratios[df.columns] = df
    df_aggregated += df["aggregated (MWh)"]

    df = nonmetalic_mineral_products()
    industry_sector_ratios[df.columns] = df
    df_aggregated += df["aggregated (MWh)"]

    df, df_totals = basic_chemicals() 
    # TODO: steam processing now part of HVC - add to aggregated?
    industry_sector_ratios[df.columns] = df
    df_aggregated += df["aggregated (MWh)"]

    df = other_chemicals()
    industry_sector_ratios[df.columns] = df
    df_aggregated += df["aggregated (MWh)"]
    
    df = iron_and_steel() 
    industry_sector_ratios[df.columns] = df
    df_aggregated += df["aggregated (MWh)"]

    industry_sector_ratios.drop("aggregated (MWh)", axis = 1, inplace = True)   

    idees_to_agora = {'Other industrial sectors': "Other non-classified",
       'Wood and wood products': "Other non-classified", 
       'Textiles and leather': "Other non-classified", 
       'Transport equipment': "Engineering and other metal",
       'Machinery equipment': "Engineering and other metal", 
       'Pulp production': "Paper and printing", 
       'Paper production': "Paper and printing",
       'Printing and media reproduction': "Paper and printing", 
       'Food, beverages and tobacco': "Food, drink and tobacco",
       'Alumina production': "Non-ferrous metals", 
       'Aluminium - primary production': "Non-ferrous metals",
       'Aluminium - secondary production': "Non-ferrous metals",
       'Aluminium - secondary production - electric': "Non-ferrous metals",
       'Other non-ferrous metals': "Non-ferrous metals",
       "Cement" : "Non-metallic mineral products", 
       'Ceramics & other NMM': "Non-metallic mineral products",
       'Glass production': "Non-metallic mineral products", 
       'Basic chemicals': "Chemical industry", 
       'Basic chemicals | NH3 + SMR': "Chemical industry",
       'Basic chemicals | methanol': "Chemical industry",
       'Basic chemicals | Cl2': "Chemical industry",
       'Basic chemicals | HVC': "Chemical industry", 
       'Basic chemicals | HVC mechanical recycling': "Chemical industry",
       'Basic chemicals | HVC chemical recycling': "Chemical industry", 
       'Basic chemicals | NH3': "Chemical industry",
       'Other chemicals': "Chemical industry", 
       'Pharmaceutical products etc.': "Chemical industry", 
       'Electric arc': "Iron and steel",
       'DRI + Electric arc': "Iron and steel", 
       'Integrated steelworks': "Iron and steel"}

    temperature = pd.read_excel("../data/raw/IDEES_by_temperature_level.xlsx", sheet_name="Per_subsector_in_percent", index_col = 0)
    # contains matched data of AGORA study () and IDEES data on process heat by temperature level
    # for each subsector, the percentage of heat demand below 100°C, 100-150°C, 150-200°C, 200-500°C, and above 500°C is given by AGORA study for EU27
    # in IDEES EU27 data process heat and furnace heat is given for EU27
    # matching for every subsector:
    #  1. subtract furnace heat (IDEES) from furnace heat (AGORA) 
    #  2. calculate per-temperature-shares for the remaining process heat (AGORA)
    #  3. multiply IDEES shares with process heat from 2 to get process heat shares by temperature level (IDEES)

    for c in industry_sector_ratios.columns:
        # below 150 °C (heat pumps possible)
        ct = idees_to_agora[c]
        perc = (temperature.loc[ct, "<100°C"] + temperature.loc[ct, "100-150°C"]) 
        industry_sector_ratios.loc["lowT process heat", c] = industry_sector_ratios.loc["process heat", c]*perc
        # below 150 °C (no heat pumps possible, but electric heating)
        perc = (temperature.loc[ct, "200-500°C"] + temperature.loc[ct, "150-200°C"]) 
        industry_sector_ratios.loc["mediumT process heat", c] = industry_sector_ratios.loc["process heat", c]*perc
        # above 500 °C (TODO: check possibilities, now no electric heating)
        perc = (temperature.loc[ct, ">500°C"]) 
        industry_sector_ratios.loc["highT process heat", c] = industry_sector_ratios.loc["process heat", c]*perc

    industry_sector_ratios.loc["production (kt)", ["Basic chemicals | NH3 + SMR", "Basic chemicals | methanol", "Basic chemicals"]] = 0
    alu_total = industry_sector_ratios.loc["production (kt)", ["Aluminium - secondary production - electric", "Aluminium - secondary production", "Aluminium - primary production"]].sum()
    industry_sector_ratios.loc["production (kt)", "Aluminium - secondary production - electric"] = (1-Al_primary_fraction[2045])*alu_total
    industry_sector_ratios.loc["production (kt)", "Aluminium - primary production"] = (Al_primary_fraction[2045])*alu_total

    return industry_sector_ratios, df_aggregated

def plot_current_energy_demand():
    import matplotlib.pyplot as plt 
    industry_sector_ratios, df_aggregated = new_industry_sector_ratios()
    #fig, axes = plt.subplots(2,1, sharex = True)
    totals_fec = industry_sector_ratios.loc["elec": "naphtha", :]*industry_sector_ratios.loc["production (kt)", :]
    totals_fec.T.plot(kind = "bar", stacked = True) #, ax = axes[0])
    plt.ylabel("Final energy demand in MWh")
    totals_ued = industry_sector_ratios.loc["lowT process heat": "furnaces heat", :]*industry_sector_ratios.loc["production (kt)", :]
    totals_ued.T.plot(kind = "bar", stacked = True) #, ax = axes[0])
    plt.ylabel("Heat demand in MWh")
    df_aggregated.T.plot(kind = "bar", stacked = True)
    plt.ylabel("Final energy demand for space heat and auxiliary in MWh")


def exogenous_industry_demand(endogenous_industry_list):
    '''
    endogenous_industry_list: list of industries that are endogenous to the model'''
    industry_sector_ratios, df_aggregated = new_industry_sector_ratios()
    totals = industry_sector_ratios.loc["elec": "furnaces heat", :]*industry_sector_ratios.loc["production (kt)", :]*1000 #kt->t
    exo_industry_list  = industry_sector_ratios.columns.drop(endogenous_industry_list).drop(['Basic chemicals | NH3 + SMR']).to_list()
    df_aggregated.loc["elec": "furnaces heat"] += totals[exo_industry_list].sum(axis = 1)
    return df_aggregated   

def DE_all_industry_demand(endogenous_industry_list):
    industry_sector_ratios, df_aggregated = new_industry_sector_ratios()
    esm_industry_sector_ratios = industry_sector_ratios.copy()
    steel_total = industry_sector_ratios.loc["production (kt)", ["Electric arc", "DRI + Electric arc", "Integrated steelworks"]].sum()
    esm_industry_sector_ratios.loc["production (kt)", "Electric arc"] = steel_total*(1-St_primary_fraction[2045])
    esm_industry_sector_ratios.loc["production (kt)", "DRI + Electric arc"] = steel_total*St_primary_fraction[2045]*DRI_fraction[2045]
    esm_industry_sector_ratios.loc["production (kt)", "Integrated steelworks"] = steel_total*St_primary_fraction[2045]*(1-DRI_fraction[2045])
    totals = esm_industry_sector_ratios.loc["elec": "furnaces heat", :]*esm_industry_sector_ratios.loc["production (kt)", :]*1000 #kt->t
    esm_industry_sector_ratios.columns.drop(['Basic chemicals | NH3 + SMR']).to_list()
    df_aggregated.loc["elec": "furnaces heat"] += totals.drop(endogenous_industry_list, axis = 1).sum(axis = 1)
    df_agggregated_endogenous = totals[endogenous_industry_list].sum(axis = 1)
    return df_aggregated, df_agggregated_endogenous, totals  

def industry_to_subtract(industry_to_subtract_list):
    industry_sector_ratios, df_aggregated = new_industry_sector_ratios()
    industry_sector_ratios.loc["production (kt)", "Electric arc"] = 0.65*(industry_sector_ratios.loc["production (kt)", "Electric arc"] + industry_sector_ratios.loc["production (kt)", "Integrated steelworks"])
    industry_sector_ratios.loc["production (kt)", "DRI + Electric arc"] = 0.35*(industry_sector_ratios.loc["production (kt)", "Electric arc"] + industry_sector_ratios.loc["production (kt)", "Integrated steelworks"])
    totals = industry_sector_ratios.loc["elec": "furnaces heat", :]*industry_sector_ratios.loc["production (kt)", :]*1000
    return totals[industry_to_subtract_list].sum(axis = 1) 
###################################################################
# create output
country = "DE"
endogenous_industry_list = ['Cement', 'Basic chemicals', 'Basic chemicals | methanol', 
       'Basic chemicals | HVC', 'Basic chemicals | HVC mechanical recycling',
       'Basic chemicals | HVC chemical recycling', 
       'Electric arc', 'DRI + Electric arc', 'Integrated steelworks']

industry_to_subtract_list = ['Cement',  
       'Basic chemicals | HVC', 'Electric arc', 'DRI + Electric arc']

industry_sector_ratios, df_aggregated = new_industry_sector_ratios()
industry_sector_ratios.to_csv(f"../data/industry_sector_ratios_{country}_MWh_per_t.csv")

subtract = industry_to_subtract(industry_to_subtract_list)
#subtract.to_csv(f"../data/pypsa_eur_demand_for_endogenous_sector_{country}_MWh.csv")

# eff_coal_furnace, eff_waste_furnace, EF_coal_furnace, EF_waste_furnace = cement_highT_furnace_data()
# highT_furnaces = pd.DataFrame(columns = ["ued/fec", "emi/ued"], index = ["coal", "waste"],
#                               data = {"ued/fec": [eff_coal_furnace, eff_waste_furnace], "emi/ued": [EF_coal_furnace, EF_waste_furnace]})
# highT_furnaces.to_csv("Cement_furnace_eff_emi_IDEES.csv")

df_agg, endo_agg, totals = DE_all_industry_demand(endogenous_industry_list=endogenous_industry_list)
# all except endogenous
df_agg.to_csv(f"../data/exo_yearly_industry_energy_demand_{country}_MWh.csv")
# endogenous
endo_agg.to_csv(f"../data/endo_yearly_industry_energy_demand_{country}_MWh.csv")

(df_agg+endo_agg).to_csv(f"../data/all_yearly_industry_energy_demand_{country}_MWh.csv")