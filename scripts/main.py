
###################################################
# scenario parameter (User Input)
###################################################
endogenous_material = True 

biomass_scenario = "ENS_Med"
co2_cap = 0
mat_scenario = "today"
# choose ["low", "high", "today", "manual"]
H2_importcost = 84

industry_brownfield = False
# for now only greenfield, future work

number_snapshots_for_solving = 2 
# small number of snapshots for test-solving, 
# change to "all" to fully solve it

# set parameters for endogenous_material = True 
local_prod_perc = 1
# 1 means all technologies domestically produced, 
# 0 means all technologies imported

####################################################
# name for saving results
####################################################
name = f"Mat_{mat_scenario}_H2_{H2_importcost}"
if endogenous_material:
    name += f"_local_{local_prod_perc}"

############################################################
# Imports and Setup
############################################################
import pypsa as pypsa
import pandas as pd
import numpy as np
import os
from pypsa.optimization.compat import define_constraints, get_var, join_exprs, linexpr
from Industry_input import steel_input, cement_input, chem_input
from Industry_model import industry_module, add_process_heat
from technologies_material_demand import prepare_technology_material_demands,\
    solve_industry_module_mat, fix_neighbouring_countries_capacities, correct_capital_costs, fix_H2_imports

############################################################
# Load Input Data and basic network
############################################################

file_path = "../data/industry_sector_ratios_DE_MWh_per_t.csv"
if os.path.exists(file_path):
    DE_industry_sector_ratios = pd.read_csv(file_path, index_col = 0)
else:
    print(f"{file_path} does not exist, run scripts-pre_processing/DE_build_industry_sector_ratios.py")  
    
steel_load, steel_load_today, steel_feedstock, steel_energy, steel_cost, steel_prod_proj, steel_todays_capacities, max_scrap, min_scrap = steel_input(use_pypsa_data = True, years = range(2020, 2046))
hvc_load, hvc_load_today, hvc_feedstock, hvc_energy, hvc_cost, hvc_prod_proj, max_waste, min_waste, min_packaging_waste, max_packaging_waste = chem_input(
    use_pypsa_data = True, years = range(2020, 2046))

production = pd.read_excel("../data/raw/data_industry.xlsx", sheet_name="production", index_col = 0)
methanol_load = production.loc["methanol", "production"]*1e3*6.3/8760 # kt/y -> MWh/h, for DE, source: C4C 
hvc_load = production.loc["HVC", "production"] # kt/y, for DE, from C4C (in SCI4Climate: 11000 kt in 2050)

cement_load, cement_load_today, cement_feedstock, cement_energy, cement_cost = cement_input(
    use_pypsa_data = True, years = range(2020, 2046))

networkname = "../data/pre-networks-input/endogenousind/elec_s_15_lv1.5__Co2L0-3H-T-H-B-I-A-solar+p3-dist1_2045.nc"
n = pypsa.Network(networkname)

#costs = pd.read_csv("../data/prepared_costs.csv", index_col = 0)
costs = pd.read_csv("../data/pypsa-eur/costs_2045.csv", index_col = [0,1], delimiter = ";")["value"]

file_path = f"../data/biomass_potentials_DE_2040_{biomass_scenario}.csv"
if os.path.exists(file_path):
    bio = pd.read_csv(file_path, index_col = 0)
    biocost = pd.read_csv(f"../data/biomass_cost_DE_2040_{biomass_scenario}.csv", index_col = 0)
else:
    print(f"{file_path} does not exist, run scripts-pre_processing/biomass.py")  

# Load industry energy demand (exogenous sectors)
if os.path.exists("../data/exo_yearly_industry_energy_demand_DE_MWh.csv"):
    DE_exogenous_industry_demand = pd.read_csv("../data/exo_yearly_industry_energy_demand_DE_MWh.csv", index_col=0)['0']
else:
    print("../data/exo_yearly_industry_energy_demand_DE_MWh.csv does not exist, run scrips_preprocessing/DE_build_industry_sector_ratios.py")

if os.path.exists("../data/all_yearly_industry_energy_demand_DE_MWh.csv"):
    DE_industry_demand = pd.read_csv("../data/all_yearly_industry_energy_demand_DE_MWh.csv", index_col=0)['0']
else:
    print("../data/all_yearly_industry_energy_demand_DE_MWh.csv does not exist, run scripts_preprocessing/DE_build_industry_sector_ratios.py")

############################################################
# set material demands based on scenario
############################################################

if mat_scenario == "low":
    cement_output = 18390.567717 #kt 55% CLEVER nature paper, SI Dashboard
    steel_output =  35473.431893 #kt 83 % CLEVER nature paper, SI Dashboard
    hvc_output *= 0.6 # -40% in other European countries, CLEVER Industry paper
    # CLEVER scenario data
    # data here: https://zenodo.org/records/11546125
    # file for DE: Inputs/Dashboard_DE_v5.22_AG_clean.xlsx
    # saved in industry_subsectors folder
if mat_scenario == "high":
    #scenarios usually take reduced production quantities (see ESYS). Here, constant production quantities -> conservative
    # sensitivity up 20%
    cement_output *= 1.2
    steel_output *= 1.2
    hvc_output *= 1.2
if mat_scenario == "today" or "":
    pass

############################################################
# Helper Functions
############################################################
def annuity(lifetime, dr):
    return (1 - (1 + dr) ** -lifetime) / dr

def replace_nan_in_links(n):
        # Replace NaN values in n.links.bus with empty strings
        for c in n.links.filter(like = "bus").columns:
            n.links[c].fillna("", inplace=True)
        # Replace NaN values in n.links.efficiency with empty strings
        for c in n.links.filter(like = "efficiency").columns:
            n.links[c].fillna(0, inplace=True)

############################################################
# Add and Configure Industry Buses and Links
############################################################
def add_and_configure_industry_buses_and_links():
    # Add buses for DE: (1) on node-level: process emissions, (2) country-level: gas, oil,...
    # (1) node-level
    nodes = ["DE1 0"]
    for node in nodes:
        n.add("Bus", node + " process emissions", carrier="process emissions")

    # (2) country-level buses: gas, oil, methanol, solid biomass, biogas, co2 atmosphere, co2 stored
    node = "DE"
    buses = [
        (node + " gas", "gas"),
        (node + " solid biomass", "solid biomass"),
        (node + " biogas", "biogas"),
        (node + " co2 atmosphere", "co2"),
        (node + " co2 stored", "co2 stored"),
        (node + " methanol", "methanol"),
        (node + " oil", "oil"),
        (node + " gas for industry", "gas for industry")
    ]
    
    for bus_name, carrier in buses:
        n.add("Bus", bus_name, carrier=carrier)

    # Add DE generators for gas and oil
    df_gen = pd.DataFrame(columns=n.generators.columns)
    df_gen.loc["DE gas"] = n.generators.loc["EU gas"].copy()
    df_gen.loc["DE gas", "bus"] = "DE gas"
    df_gen.loc["DE oil"] = n.generators.loc["EU oil"].copy()
    df_gen.loc["DE oil", "bus"] = "DE oil"
    
    # Add links: DE gas for industry (CC), DE process emissions (CC)
    elec_demand_ccs = 1.1
    n.add("Link", "DE gas for industry", bus0 = "DE gas", bus1 = "DE gas for industry", bus2 = "DE co2 atmosphere",
        efficiency = 1, efficiency2 = 0.2, carrier = "gas for industry", p_nom_extendable = True)  
    n.add("Link", "DE gas for industry CC", bus0 = "DE gas", bus1 = "DE gas for industry", bus2 = "DE co2 stored", bus3 ="DE1 0",
        efficiency = 1, efficiency2 = 0.2, efficiency3 = -elec_demand_ccs, carrier = "gas for industry CC", 
        p_nom_extendable = True) 
    
    df_node_links = pd.DataFrame(columns = n.links.columns)
    country = "DE"
    nodes = ["DE1 0"]
    for node in nodes:
        prefix = node
        link = "process emissions"
        df_node_links.loc[prefix+ " "+link] = n.links.loc[link]
        df_node_links.loc[prefix+ " "+link, "bus0"] = prefix+" process emissions"
        df_node_links.loc[prefix+ " "+link, "bus1"] = country+" co2 atmosphere"
        link = "process emissions CC"
        df_node_links.loc[prefix+ " "+link] = n.links.loc["EU " + link]
        df_node_links.loc[prefix+ " "+link, "bus0"] = prefix+" process emissions"
        df_node_links.loc[prefix+ " "+link, "bus1"] = country+" co2 atmosphere"
        df_node_links.loc[prefix+ " "+link, "bus2"] = country+" co2 stored"
        df_node_links.loc[prefix+ " "+link, "bus3"] = prefix # add elec. demand CC
        df_node_links.loc[prefix+ " "+link, "efficiency3"] = - elec_demand_ccs # add elec. demand CC
    n.import_components_from_dataframe(df_node_links, "Link")

    # for shadow prices: connect emissions to source
    # by replacing loads with links
    replace_emission_loads_by_links = {
        # oil emissions are from oil for industry and kerosene, see pypsa-eur:
        # https://github.com/PyPSA/pypsa-eur-sec/blob/master/scripts/prepare_sector_network.py
        # co2_release = ["naphtha for industry", "kerosene for aviation"]
        
        "oil for industry": {
            "bus0": "EU oil",
            "bus1": "EU oil for industry",
            "bus2": "co2 atmosphere",
            "load": "naphtha for industry",
            "emissions load": "oil emissions"
        },
        "kerosene for aviation": {
            "bus0": "EU oil",
            "bus1": "EU oil for aviation",
            "bus2": "co2 atmosphere",
            "load": "kerosene for aviation",
            "emissions load": "oil emissions"
        },
        "oil for agri": {
            "bus0": "EU oil",
            "bus1": "EU oil for agri",
            "bus2": "co2 atmosphere",
            "load": "agriculture machinery oil",
            "emissions load": "agriculture machinery oil emissions"
        },
        "methanol for shipping": {
            "bus0": "EU methanol",
            "bus1": "EU methanol for shipping",
            "bus2": "co2 atmosphere",
            "load": "EU methanol shipping methanol",
            "emissions load": "shipping methanol emissions"
        }
    }

    for key, value in replace_emission_loads_by_links.items():
        if key == "methanol for shipping":
            emi = -n.loads.loc[value["emissions load"], "p_set"] / n.loads.loc[value["load"], "p_set"]
        else:    
            emi = costs.at["oil", "CO2 intensity"]
        n.add("Bus", value["bus1"], carrier=key)
        n.add("Link", key, bus0=value["bus0"],
               bus1=value["bus1"], bus2=value["bus2"], carrier=key,
               efficiency=1, p_nom_extendable=True,
               efficiency2 = emi)
        n.loads.loc[value["load"], "bus"] = value["bus1"]
    n.mremove("Load", ["agriculture machinery oil emissions", "shipping methanol emissions", "oil emissions"])

    # Change buses for industry links
    bus_replacements = {
        "EU solid biomass": "DE solid biomass",
        "EU gas": "DE gas",
        "EU methanol": "DE methanol",
        "EU oil": "DE oil",
        "co2 atmosphere": "DE co2 atmosphere",
        "co2 stored": "DE co2 stored",
    }

    # Function to replace buses in links
    def replace_buses_in_links(links_idx, bus_col, replacements):
        for old_bus, new_bus in replacements.items():
            idx = n.links.loc[links_idx][n.links.loc[links_idx, bus_col] == old_bus].index
            n.links.loc[idx, bus_col] = new_bus

    # Identify relevant links
    de_c_links = n.links.filter(like="DE ", axis=0).index
    de_n_links = n.links.filter(like="DE1 0", axis=0).index
    # Replace buses for each relevant link category
    for bus_col in ["bus0", "bus1", "bus2", "bus3", "bus4"]:
        replace_buses_in_links(de_c_links, bus_col, bus_replacements)
        replace_buses_in_links(de_n_links, bus_col, bus_replacements)

    def change_industry_to_DE():
        # change bus of industrial electricity loads (before: low voltage)
        n.loads.loc["DE1 0 industry electricity", "bus"] = "DE1 0"
        n.links.loc["DE1 0 industrial heat pump steam for lowT industry", "bus0"] = 'DE1 0'
    change_industry_to_DE()
    
    # Addition of stores for CO2, gas, oil, and methanol
    DE_limits = pd.read_excel("../data/raw/data_industry.xlsx", sheet_name="limits", index_col = 0)
    stores_data = [
        ("DE co2 stored", "DE co2 stored", DE_limits.loc["CCS DE", "limit"], True, "co2 stored", False),
        ("DE co2 atmosphere", "DE co2 atmosphere", np.inf, True, "co2", False),
        ("DE gas", "DE gas", DE_limits.loc["gas storage DE", "limit"], False, "gas", True),
        ("DE oil", "DE oil", 1e8, False, "oil", True),
        ("DE methanol", "DE methanol", 1e8, False, "methanol", True)
    ]

    for store_name, bus, e_nom, e_nom_extendable, carrier, e_cyclic in stores_data:
        n.add("Store", store_name, 
              bus=bus, 
              e_nom=e_nom,       # relevant for non-ext.
              e_nom_max=e_nom,   # relevant for ext.
              e_nom_extendable=e_nom_extendable, 
              carrier=carrier, 
              e_cyclic=e_cyclic)
    n.stores.loc["DE co2 atmosphere", "e_min_pu"] = -1
    n.stores.loc["DE co2 atmosphere", "e_initial"] = 0
add_and_configure_industry_buses_and_links()

########################################################
# Add exogenous industry loads for DE
########################################################

nodes = n.buses[n.buses.carrier == "AC"].filter(like = "DE", axis = 0).index
# set industrial loads for exogenous sectors    
n.loads.loc["DE1 0 industry electricity", "p_set"] = DE_exogenous_industry_demand.loc["elec"]/8760
n.loads.loc["DE1 0 H2 for industry", "p_set"] = DE_exogenous_industry_demand.loc["hydrogen"]/8760
n.loads.loc["DE1 0 low-temperature heat for industry", "p_set"] = DE_exogenous_industry_demand.loc["heat"]/8760
n.loads.loc["DE1 0 lowT industry", "p_set"] = DE_exogenous_industry_demand.loc["lowT process heat"]/8760
n.loads.loc["DE1 0 mediumT industry", "p_set"] = DE_exogenous_industry_demand.loc["mediumT process heat"]/8760
n.loads.loc["DE1 0 highT industry", "p_set"] = DE_exogenous_industry_demand.loc["highT process heat"]/8760 + DE_exogenous_industry_demand.loc["furnaces heat"]/8760

n.add("Load", "DE industry methane", bus = "DE gas for industry", carrier = "industry methane", p_set = DE_exogenous_industry_demand.loc["methane"]/8760)
n.madd("Load", nodes + " industry process emissions", bus = nodes + " process emissions", carrier = "industry process emissions", 
       p_set = -DE_exogenous_industry_demand.loc["process emission"]/8760)

# remove DE loads from EU loads
n.loads.loc["naphtha for industry", "p_set"] -= DE_industry_demand.loc["naphtha"] / 8760
n.loads.loc["process emissions", "p_set"] += DE_industry_demand.loc["process emission"] / 8760    

############################################################
# Add DAC
############################################################
def add_dac(n, elec_demand_dac = (0.47 + 1.8)):
    n.add("Link", name="DE DAC",
          bus0="DE co2 atmosphere", 
          bus1="DE co2 stored", 
          bus2="DE1 0",
          carrier="DAC",
          capital_cost=863357.5,
          marginal_cost=0,
          p_nom_extendable=True,
          efficiency=1,
          efficiency2=-elec_demand_dac, 
          lifetime=20.0)
    for node in n.buses[n.buses.carrier == "AC"].index:
        if node != "DE1 0":
            n.add("Link", name=f"{node} DAC",
                  bus0="co2 atmosphere", 
                  bus1="co2 stored", 
                  bus2=node,
                  carrier="DAC",
                  capital_cost=863357.5,
                  marginal_cost=0,
                  p_nom_extendable=True,
                  efficiency=1,
                  efficiency2=-elec_demand_dac, 
                  lifetime=20.0)
    return n
add_dac(n)

############################################################
# Remove EV flexibility
############################################################
idx = n.links.filter(like = "V2G", axis = 0).index
if len(idx) > 0:
    n.mremove("Link", idx)  
idx = n.stores.filter(like = "battery storage", axis = 0).index
if len(idx) > 0:
    n.mremove("Store", idx) 

############################################################
# Adjust parameter
############################################################
n.generators.loc["DE1 0 offwind-ac", "p_nom_max"] =  27.7*1e3
n.generators.loc["DE1 0 offwind-dc", "p_nom_max"] =  44.7*1e3 
# for comparison: ESYS 5*1e3*25 -> 5 GW/a ESYS, 25 years (2020->2045)

#https://energy.ec.europa.eu/news/eu-reaches-90-gas-storage-target-10-weeks-ahead-deadline-2024-08-21_en
n.stores.loc["EU gas Store", "e_nom"] = 1025/0.9*1e6 #MWh storage capacity
n.stores.loc["EU gas Store", "e_nom_extendable"] = False

############################################################
# Add H2 turbines and H2 imports
############################################################
if len(n.links[n.links.carrier == "H2 turbine"].index) == 0:
    for node in n.buses[n.buses.carrier == "AC"].index:    
        n.add(
            "Link",
            node + " H2 turbine",
            bus0=node + " H2",
            bus1=node,
            p_nom_extendable=True,
            carrier="H2 turbine",
            efficiency=costs.at["OCGT", "efficiency"],
            capital_cost=costs.at["OCGT", "investment"]
            * costs.at["OCGT", "efficiency"],  # NB: investment cost is per MWel
            marginal_cost=costs.at["OCGT", "VOM"],
            lifetime=costs.at["OCGT", "lifetime"],
        )

print(f"add H2 import: all countries in scope have ship import with price {H2_importcost} €/MWh")        
for node in n.buses[n.buses.carrier == "AC"].index:
    n.add("Generator", node+" H2 import", bus = node+" H2", 
            p_nom_extendable = True, marginal_cost = H2_importcost, 
            carrier = "H2 import")
############################################################
# Add biomass and waste, ENSPRESO 2040 Med scenario
############################################################
 
digestible_biomass_types = ["manureslurry", "straw"]
solid_biomass_types = ["forest residues", "industry wood residues", "landscape care"]
def aggregate_biomass_de(bio):
    return bio.filter(like = "DE", axis = 0).drop("DE").fillna(0).sum(axis = 0)
def average_biomass_de(bio):
    return bio.filter(like = "DE", axis = 0).drop("DE").fillna(0).mean(axis = 0)
bio_de = aggregate_biomass_de(bio)
biocost_de = average_biomass_de(biocost)
DE_costs = {"solid biomass": biocost_de[solid_biomass_types].mean(),
            "digestible biomass": biocost_de[digestible_biomass_types].mean(),
            "municipal solid waste": biocost_de["municipal solid waste"].mean(),
            "CCS": 0}

DE_limits = {"solid biomass": bio_de[solid_biomass_types].sum(),
            "digestible biomass": bio_de[digestible_biomass_types].sum(),
            "municipal solid waste": bio_de["municipal solid waste"].sum(),
            "CCS": 0}

# add biomass stores 
n.add("Store", "DE solid biomass", 
    bus = "DE solid biomass", 
    e_nom = DE_limits["solid biomass"], 
    e_initial = DE_limits["solid biomass"],
    e_nom_extendable = False, 
    marginal_cost = DE_costs["solid biomass"],
    carrier = "solid biomass")

n.add("Store", "DE digestible biomass", 
    bus = "DE biogas", 
    e_nom = DE_limits["digestible biomass"], 
    e_initial = DE_limits["digestible biomass"], 
    e_nom_extendable = False,
    marginal_cost = DE_costs["digestible biomass"],
    carrier = "biogas")
# change limits for EU bio stores

n.stores.loc["EU solid biomass", ["e_initial", "e_nom"]] = np.max((n.stores.loc["EU solid biomass", "e_initial"] - n.stores.loc["DE solid biomass", "e_initial"], 0)) 
#n.stores.loc["EU biogas", ["e_initial", "e_nom"]] = np.max(n.stores.loc["EU biogas", "e_initial"] - n.stores.loc["DE digestible biomass", "e_initial"], 0)        

n.stores.loc["EU biogas", "marginal_cost"] = n.stores.loc["DE digestible biomass", "marginal_cost"]
n.stores.loc["EU solid biomass", "marginal_cost"] = n.stores.loc["DE solid biomass", "marginal_cost"]

n.add("Carrier", "municipal solid waste")
n.add("Bus",
    "DE municipal solid waste",
    carrier="municipal solid waste")
n.add("Store",
        "DE municipal solid waste",
        bus="DE municipal solid waste",
        carrier="municipal solid waste",
        e_nom = DE_limits["municipal solid waste"], 
        e_initial = DE_limits["municipal solid waste"], 
        e_nom_extendable = False,
        marginal_cost = DE_costs["municipal solid waste"])

# add biomass links
n.add("Link",
        "DE" + " biogas",
        bus0="DE" + " biogas",
        bus1="DE gas",
        bus2="DE co2 atmosphere",
        carrier="biogas to gas",
        capital_cost=n.links.loc["EU biogas to gas", "capital_cost"],
        #(costs.at["biogas", "investment"] + costs.at["biogas", "FOM"] + costs.at["biogas upgrading", "FOM"]) * costs.at["biogas","efficiency"],
        marginal_cost=(costs.at["biogas upgrading", "VOM"] * costs.at["biogas","efficiency"]),
        efficiency=costs.at["biogas","efficiency"],
        efficiency2=-costs.at['gas', 'CO2 intensity'] * costs.at["biogas","efficiency"],
        p_nom_extendable=True)#,
        #p_nom_max = n.stores.loc["DE digestible biomass", "e_nom"])
#biogas to gas CC not an extra link because it is biogas to gas + gas for industry CC
# n.links.loc["EU biogas to gas", "capital_cost"] = n.links.loc["DE biogas", "capital_cost"]

n.add("Link",
        "DE" + " solid biomass to gas",
        bus0="DE" + " solid biomass",
        bus1="DE gas",
        bus3="DE co2 atmosphere",
        carrier="BioSNG",
        lifetime=costs.at['BioSNG', 'lifetime'],
        efficiency=costs.at['BioSNG', 'efficiency'],
        efficiency3=-costs.at['solid biomass', 'CO2 intensity'] + costs.at['BioSNG', 'CO2 stored'],
        p_nom_extendable=True,
        capital_cost=costs.at['BioSNG', 'investment'] * costs.at['BioSNG', 'efficiency'],
        marginal_cost=costs.at['BioSNG', 'efficiency']*costs.loc["BioSNG", "VOM"]
        )

n.add("Link",
        "DE" + " solid biomass to gas CC",
        bus0="DE" + " solid biomass",
        bus1="DE gas",
        bus2="DE co2 stored",
        bus3="DE co2 atmosphere",
        carrier="BioSNG CC",
        lifetime=costs.at['BioSNG', 'lifetime'],
        efficiency=costs.at['BioSNG', 'efficiency'],
        efficiency2=costs.at['BioSNG', 'CO2 stored'] * 0.9,
        efficiency3=-costs.at['solid biomass', 'CO2 intensity'] + costs.at['BioSNG', 'CO2 stored'] * (1 - 0.9),
        p_nom_extendable=True,
        capital_cost=costs.at['BioSNG', 'investment'] * costs.at['BioSNG', 'efficiency'] + costs.at['biomass CHP capture', 'investment'] * costs.at[
            "BioSNG", "CO2 stored"],
        marginal_cost=costs.at['BioSNG', 'efficiency']*costs.loc["BioSNG", "VOM"]
        )
nodes = "DE1 0"
n.add("Link",
        nodes + " solid biomass to hydrogen CC",
        bus0="DE" + " solid biomass",
        bus1=nodes + " H2",
        bus2="DE co2 stored",
        bus3="DE co2 atmosphere",
        carrier="solid biomass to hydrogen CC",
        efficiency=costs.at['solid biomass to hydrogen', 'efficiency'],
        efficiency2=costs.at['solid biomass', 'CO2 intensity'] * 0.9,
        efficiency3=-costs.at['solid biomass', 'CO2 intensity'] + costs.at['solid biomass', 'CO2 intensity'] * (1 - 0.9),
        p_nom_extendable=True,
        capital_cost=costs.at['solid biomass to hydrogen', 'investment'] * costs.at['solid biomass to hydrogen', 'efficiency']
                    + costs.at['biomass CHP capture', 'investment'] * costs.at['solid biomass', 'CO2 intensity'],
        marginal_cost=0,
        )

n.add("Link",
        "DE" + " biomass to liquid",
        bus0="DE" + " solid biomass",
        bus1="DE"+" oil",
        bus3="DE co2 atmosphere",
        carrier="biomass to liquid",
        lifetime=costs.at['BtL', 'lifetime'],
        efficiency=costs.at['BtL', 'efficiency'],
        efficiency3=-costs.at['solid biomass', 'CO2 intensity'] + costs.at['BtL', 'CO2 stored'],
        p_nom_extendable=True,
        capital_cost=costs.at['BtL', 'investment'] * costs.at['BtL', 'efficiency'],
        marginal_cost=costs.at['BtL', 'efficiency']*costs.loc["BtL", "VOM"]
        )

n.add("Link",
    "DE" + " biomass to liquid CC",
    bus0="DE" + " solid biomass",
    bus1="DE"+" oil",
    bus2="DE co2 stored",
    bus3="DE co2 atmosphere",
    carrier="biomass to liquid CC",
    lifetime=costs.at['BtL', 'lifetime'],
    efficiency=costs.at['BtL', 'efficiency'],
    efficiency2=costs.at['BtL', 'CO2 stored'] * 0.9,
    efficiency3=-costs.at['solid biomass', 'CO2 intensity'] + costs.at['BtL', 'CO2 stored'] * (1 - 0.9),
    p_nom_extendable=True,
    capital_cost=costs.at['BtL', 'investment'] * costs.at['BtL', 'efficiency'] + costs.at['biomass CHP capture', 'investment'] * costs.at[
        "BtL", "CO2 stored"],
    marginal_cost=costs.at['BtL', 'efficiency'] * costs.loc["BtL", "VOM"]
    )

efuel_scale_factor = costs.at['BtL', 'C stored']
n.add("Link",
        nodes + " electrobiofuels",
        bus0= "DE" + " solid biomass",
        bus1= "DE"+" oil",
        bus5=nodes + " H2",
        bus3="DE co2 atmosphere",
        carrier="electrobiofuels",
        lifetime=20,
        efficiency=costs.at['electrobiofuels', 'efficiency-biomass'],
        efficiency5=-costs.at['electrobiofuels', 'efficiency-hydrogen'],
        efficiency3=-costs.at['solid biomass', 'CO2 intensity'] + costs.at['BtL', 'CO2 stored'] * (1 - costs.at['Fischer-Tropsch', 'capture rate']),
        p_nom_extendable=True,
        capital_cost=costs.at['BtL', 'investment'] * costs.at['electrobiofuels', 'efficiency-biomass'] \
                    + efuel_scale_factor * costs.at['Fischer-Tropsch', 'investment'] * costs.at['electrobiofuels', 'efficiency-hydrogen'],
        marginal_cost=costs.at['BtL', 'VOM'] * costs.at['electrobiofuels', 'efficiency-biomass'] \
                        + efuel_scale_factor * costs.at['Fischer-Tropsch', 'VOM'] * costs.at['electrobiofuels', 'efficiency-hydrogen']
        )

n.add("Link",
        nodes + " solid biomass to electricity",
        bus0= "DE" + " solid biomass",
        bus1= nodes,
        bus3="DE co2 atmosphere",
        carrier="solid biomass to electricity",
        p_nom_extendable=True,
        capital_cost=0.7 * costs.at['central solid biomass CHP', 'investment'] * costs.at[
            'central solid biomass CHP', 'efficiency'],
        marginal_cost=costs.at['central solid biomass CHP', 'VOM'],
        efficiency=1.3 * costs.at['central solid biomass CHP', 'efficiency'],
        efficiency3=costs.at['solid biomass', 'CO2 intensity']-costs.at['solid biomass', 'CO2 intensity'],
        lifetime=costs.at['central solid biomass CHP', 'lifetime'])

n.add("Link",
    nodes + " solid biomass to electricity CC",
    bus0="DE" + " solid biomass",
    bus1=nodes,
    bus2="DE co2 stored",
    bus3="DE co2 atmosphere",
    carrier="solid biomass to electricity CC",
    p_nom_extendable=True,
    capital_cost=0.7 * costs.at['central solid biomass CHP CC', 'investment'] * costs.at[
        'central solid biomass CHP CC', 'efficiency']
                + costs.at['biomass CHP capture', 'investment'] * costs.at['solid biomass', 'CO2 intensity'],
    marginal_cost=costs.at['central solid biomass CHP CC', 'VOM'],
    efficiency=1.3 * costs.at['central solid biomass CHP CC', 'efficiency'],
    efficiency2=costs.at['solid biomass', 'CO2 intensity'] * 0.9,
    efficiency3=-costs.at['solid biomass', 'CO2 intensity'] + costs.at['solid biomass', 'CO2 intensity'] * (1 - 0.9),
    # p_nom_ratio=costs.at['central solid biomass CHP', 'p_nom_ratio'],
    lifetime=costs.at['central solid biomass CHP CC', 'lifetime'])

#add waste CHP CC
urban_central = nodes 
key = "waste CHP"
n.add("Link",
        urban_central + " urban central waste CHP CC",
        bus0="DE municipal solid waste",
        bus1=urban_central,
        bus2=urban_central + " urban central heat",
        bus3="DE co2 stored",
        carrier="urban central waste CHP CC",
        p_nom_extendable=True,
        capital_cost=costs.at[key, "investment"] * costs.at[key, "efficiency"] + costs.at['biomass CHP capture', 'investment'] * 0.26,
        marginal_cost=costs.at[key, "VOM"],
        efficiency=costs.at[key, "efficiency"],
        efficiency2=costs.at[key, "efficiency-heat"],
        efficiency3 = 0.26,
        lifetime=costs.at[key, "lifetime"],
    )

#add waste CHP
urban_central = nodes 
key = "waste CHP"
n.add("Link",
        urban_central + " urban central waste CHP",
        bus0="DE municipal solid waste",
        bus1=urban_central,
        bus2=urban_central + " urban central heat",
        bus3="DE co2 atmosphere",
        carrier="urban central waste CHP",
        p_nom_extendable=True,
        capital_cost=costs.at[key, "investment"] * costs.at[key, "efficiency"],
        marginal_cost=costs.at[key, "VOM"],
        efficiency=costs.at[key, "efficiency"],
        efficiency2=costs.at[key, "efficiency-heat"],
        efficiency3 = 0.26,
        lifetime=costs.at[key, "lifetime"],
    )

############################################################
# Adjust parameters
############################################################
# methanolisation costs
an = annuity(20, 0.07)
idx = n.links[n.links.carrier == "methanolisation"].index
n.links.loc[idx, "capital_cost"] = 470000*0.8787/an

# line capacities
def limit_line_capacities(n):
    n.lines.s_nom_max = n.lines.s_nom_min*1.5
    # s_nom_min up to 27000 MW
    # dc = n.links[n.links.carrier == "DC"].index 
    # n.links.loc[dc, "p_nom_max"] = 20000 
    # MW, because of s_nom_min up to 27000 MW
limit_line_capacities(n)

# add costs co2 storage
def add_costs_co2_storage(n, cost = 1000):
    co2 = n.stores[n.stores.carrier == "co2 stored"].index
    n.stores.loc[co2, "capital_cost"] = cost
    return n
add_costs_co2_storage(n)

# change limits for EU co2 stored store
EU_limit = min(n.stores.loc["co2 stored", "e_nom_max"], 3*1e9/(2045-2020)) # from Hoffmann 2024: 3 Gt/a [source 12]
EU_limit = 200*1e6 #from config file
n.stores.loc["co2 stored", "e_nom_max"] = np.max((EU_limit - n.stores.loc["DE co2 stored", "e_nom_max"], 0))
print("Limit for EU co2 stored store: ", n.stores.loc["co2 stored", "e_nom_max"]) 

# add additional bus and links for industry oil
ft_co2_stored_per_naphtha = -n.links.loc["DE1 0 Fischer-Tropsch", "efficiency2"]/n.links.loc["DE1 0 Fischer-Tropsch", "efficiency"]
n.add("Bus", "DE oil for industry", carrier = "oil for industry")
n.add("Link", "DE oil for industry", bus0 = "DE oil", bus1 = "DE oil for industry", bus2 = "DE co2 atmosphere",
        efficiency = 1, efficiency2 = ft_co2_stored_per_naphtha, p_nom_extendable = True)

# add additional bus and links for industry methanol (co2_per_methanol to atmosphere)
co2_per_methanol = -n.links.loc["DE1 0 methanolisation", "efficiency3"]/n.links.loc["DE1 0 methanolisation", "efficiency"]
n.add("Bus", "DE methanol for industry", carrier = "methanol for industry")
n.add("Link", "DE methanol for industry", bus0 = "DE methanol", bus1 = "DE methanol for industry", bus2 = "DE co2 atmosphere", 
        efficiency = 1, efficiency2 = co2_per_methanol, p_nom_extendable = True)

############################################################
# Attach industry model
############################################################
n = industry_module(n, 
    steel_load, hvc_load, cement_load, methanol_load,
    steel_energy, steel_feedstock, steel_cost,
    hvc_energy, hvc_feedstock, hvc_cost,
    cement_energy, cement_feedstock, cement_cost,
    max_waste, max_packaging_waste, max_scrap, 
    sectors = ["steel", "hvc", "cement"])

n = add_process_heat(n)

############################################################
# function to solve: add constraints: co2 = 0 at last t
############################################################
def solve_industry_module(n, co2_cap, snapshots = n.snapshots): 
   
    # co2 constraint
    def co2_constraint(n, snapshots):
        co2_atmosphere_stores = n.stores[n.stores.carrier == "co2"].index
        for c in co2_atmosphere_stores:
            sus_co2 = linexpr((1, get_var(n, "Store", "e")[snapshots[-1], c]))
            sus_name = f"{c} cap"
            define_constraints(n, sus_co2, "<=", co2_cap, "Store", sus_name)
    def co2_de_constraint(n, s):
        sus_de_co2 = linexpr((1, get_var(n, "Store", "e")[snapshots[-1], "DE co2 atmosphere"]))
        define_constraints(n, sus_de_co2, "<=", co2_cap, "Store", "DE co2 cap")

    def extra_functionalities(n, snapshots):
        co2_constraint(n, snapshots)
        co2_de_constraint(n, snapshots)

    n.optimize(solver_name="gurobi", snapshots = snapshots, 
               extra_functionality = extra_functionalities,
               solver_options= {"threads": 4, "method": 2, "crossover": 0,
                "BarConvTol": 1e-6,
                "Seed": 123,
                "AggFill": 0,
                "PreDual": 0,
                "GURO_PAR_BARDENSETHRESH": 200,                
                "OutputFlag": 1
                },
                )
    return n

def infeasibilities(n):
    n.model.compute_infeasibilities()
    n.model.solver_model.computeIIS()
    n.model.solver_model.write("model.ilp")

#############################################################
# only needed for endogenous materials
####################################################################

if endogenous_material:
    

    #TODO: replace lifetime with costs.at["technology", "lifetime"] in the future
    lifetime = {"dac": 20, 
                # TSA-DAC Climeworks, Madhu 2021: https://doi.org/10.1038/s41560-021-00922-6
                "nuclear": 60, 
                # vanVuuren 2021 https://doi.org/10.1016/j.resconrec.2020.105200
                "fossil fuel based generators": 40, #vanVuuren 2021
                "other renewables": 30, #vanVuuren 2021
                "process heat": 15, #Millinger, SI, 312a, 312b in excel "technology data for industrial process heat"
                }
    for t in ['offwind-ac', 'offwind-dc', 'onwind', 'solar', 'solar thermal', 'CSP']:
        lifetime[t] = 25 #vanVuuren 2021
    map_carrier_to_tech = {'urban central solid biomass CHP CC': 'beccs', 
                       #'biomass to liquid': 'biomass', 'BioSNG': 'biomass', 
                       'lowT industry solid biomass CC': 'gas furnace industrial CC', 
                       'solid biomass for mediumT industry CC': 'gas furnace industrial CC', 
                       #'BioSNG CC': 'beccs', 
                       #'solid biomass to hydrogen CC': 'beccs', 
                       #'biomass to liquid CC': 'beccs', 
                       'solid biomass to electricity CC': 'beccs', 
                       'solid biomass for highT industry CC': 'gas furnace industrial CC', 
                       #'biogas to gas': 'biomass', 
                       'urban central solid biomass CHP': 'biomass', 
                       'residential rural biomass boiler': 'gas boiler 15kW', 
                       'services rural biomass boiler': 'gas boiler 50kW', 
                       'residential urban decentral biomass boiler': 'gas boiler 15kW', 
                       'services urban decentral biomass boiler': 'gas boiler 50kW', 
                       'lowT industry solid biomass': 'gas furnace industrial', 
                       'solid biomass for mediumT industry': 'gas furnace industrial', 
                        'residential rural gas boiler':'gas boiler 15kW', 
                        'services rural gas boiler':'gas boiler 50kW',
                        'residential urban decentral gas boiler':'gas boiler 15kW',
                        'services urban decentral gas boiler':'gas boiler 50kW', 
                        'urban central gas boiler': 'gas boiler 50kW',  
                        'lowT industry methane':'gas furnace industrial', 
                        'lowT industry methane CC': 'Gas|w/ CCS',
                        'gas for mediumT industry': 'gas furnace industrial', 
                        'gas for mediumT industry CC': 'gas furnace industrial CC',
                        'gas for highT industry': 'gas furnace industrial', 
                        'gas for highT industry CC': 'gas furnace industrial CC',
                       #'electrobiofuels': 'biomass', 
                       'solid biomass to electricity': 'biomass',
                       'solid biomass for highT industry': 'gas furnace industrial', 
                       'DAC': 'dac', 
                       'H2 Fuel Cell': 'fuel cell', 
                       'urban central gas CHP CC': 'Gas|w/ CCS', 
                       'urban central gas CHP': 'Gas|w/o CCS', 
                       'H2 turbine': 'Gas|w/o CCS',
                       'OCGT': 'Gas|w/o CCS', 
                       'residential rural ground heat pump': 
                       'heat pump 7 kW', 
                       'residential urban decentral air heat pump': 'heat pump 7 kW', 
                       'services rural ground heat pump': 'heat pump 15 kW', 
                       'services urban decentral air heat pump': 'heat pump 15 kW', 
                       'urban central air heat pump': 'heat pump 50 kW', 
                       'lowT industry heat pump': 'heat pump 50 kW', 
                       'offwind-ac': 'offwind', 'offwind-dc': 'offwind', 'onwind': 'onwind', 
                       'solar': 'solar', 
                       'solar rooftop': 'solar rooftop', 
                       'battery': 'Li-ion battery', 
                       'H2 Store': 'H2 Store',
                        'residential rural water tanks': 'H2 Store', 
                        'services rural water tanks': 'H2 Store',
                        'residential urban decentral water tanks': 'H2 Store',
                        'services urban decentral water tanks': 'H2 Store',
                        'urban central water tanks': 'H2 Store',
                        }
    material_df, material_data = prepare_technology_material_demands(n, local_prod_perc) 
    correct_capital_costs(n, material_df)
    # if local_prod_perc > 0:
    #     fix_neighbouring_countries_capacities(n)
    # if fix_H2_imports:
    #     fix_H2_imports(n)
############################################################
# Export and test solve
############################################################
replace_nan_in_links(n)
network_path = "../results/pre-networks/"
n.export_to_netcdf(network_path + name + ".nc")  
sn = n.snapshots[0:number_snapshots_for_solving]
if number_snapshots_for_solving == "all":
    sn = n.snapshots

if endogenous_material:
    ns = solve_industry_module_mat(n, local_prod_perc, material_df, material_data, co2_cap = co2_cap, snapshots = sn)
else:
    ns = solve_industry_module(n, co2_cap = co2_cap, snapshots = sn)
s.export_to_netcdf(network_path + name + "_solved.nc")
