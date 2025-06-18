

#############################################################
#############################################################
# only needed for endogenous materials
#############################################################
#############################################################
import pypsa as pypsa
import pandas as pd
import numpy as np
import os
from pypsa.optimization.compat import define_constraints, get_var, linexpr
#from linopy import linexpr


#############################################################
# prepare parameters for technologie's material factors
#############################################################
def annuity(lifetime, dr):
    return (1 - (1 + dr) ** -lifetime) / dr

def prepare_technology_material_demands(n, local_prod_perc): 
        
    # commodity prices for imported materials
    # eur / ton
    # steel: Neumann 2024
    # hvc: https://tradingeconomics.com/commodities
    # cement: https://businessanalytiq.com/procurementanalytics/index/cement-price-index/
    comm_prices_2020 = {"steel": 500 ,       # from Fig. 4
                        "hvc": 5726*0.1315,  # january, polyethylene, cny->eur
                        "cement": 84.11      # january, Europe Actual 
    }
    
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
    ratio_hvc_plastics = 1 # TODO, for now chatGPT citing IEA
    ratio_cement_concrete = 0.13 # VDZ 2022 Ressourcen Abb.15
    ratio_iron_steel = 1 # because titled "iron and steel" in the data, but then called Iron in the code (Arvesen 2018)

    material_data = pd.read_excel("../data_material_factors/data_industry.xlsx", sheet_name="material factors", index_col = 0).iloc[:,:3]
    material_data = material_data.groupby(by = ["technology", "material"]).max()
    material_data = material_data.reset_index()
    # convert concrete to cement quantity
    row_numbers = material_data[material_data['material'] == "concrete"].index
    material_data.loc[row_numbers, "value"] *= ratio_cement_concrete
    material_data.loc[row_numbers, "material"] = "cement"
    # convert plastics to hvc
    row_numbers = material_data[material_data['material'] == "plastic"].index
    material_data.loc[row_numbers, "value"] *= ratio_hvc_plastics
    material_data.loc[row_numbers, "material"] = "hvc"
    #  convert iron to steel
    row_numbers = material_data[material_data['material'] == "iron"].index
    material_data.loc[row_numbers, "value"] *= ratio_iron_steel
    material_data.loc[row_numbers, "material"] = "steel"
    # convert to pypsa technology names
    material_data.index = material_data.technology
    df = pd.DataFrame(index = map_carrier_to_tech.keys(),
                 columns = ["capacity unit", "p_nom_carrier", "multiply with efficiency", "multiply with","steel", "cement", "hvc", 
                            "an capital cost", "material cost per capacity unit", "lifetime", "an material cost per capacity unit", "tech"])
    df["tech"] = map_carrier_to_tech.values()
    df["multiply with"] = 1 
    # 1:1 for all heat technologies (100 % in furnaces)

    for i in df.index:
        df.loc[i, "capacity unit"] = material_data.loc[df.loc[i, "tech"], "unit"].iloc[0]
        df.loc[i,"steel"] = material_data[material_data.material == "steel"].loc[df.loc[i, "tech"], "value"]
        df.loc[i,"cement"] = material_data[material_data.material == "cement"].loc[df.loc[i, "tech"], "value"]
        df.loc[i,"hvc"] = material_data[material_data.material == "hvc"].loc[df.loc[i, "tech"], "value"]
        df.loc[i, "an capital cost"] = n.links[n.links.carrier == i].capital_cost.mean()
        df.loc[i, "lifetime"] = n.links[n.links.carrier == i].lifetime.mean()
        if i in ['offwind-dc', 'offwind-ac', 'onwind', 'solar', 'solar thermal']:
            df.loc[i, "lifetime"] = 25 # vanVuuren 2021
            df.loc[i, "an capital cost"] = n.generators[n.generators.carrier == i].capital_cost[0]
        if i in ['H2 Store', 'battery', #'residential rural water tanks', 'services rural water tanks',
        'urban decentral water tanks',
        'rural water tanks',
        'urban central water tanks']:
            df.loc[i, "lifetime"] = n.stores[n.stores.carrier == i].lifetime[0]
            df.loc[i, "an capital cost"] = n.stores[n.stores.carrier == i].capital_cost[0]
        if len(n.links[n.links.carrier == i]) > 0:
            df.loc[i, "p_nom_carrier"] = n.links[n.links.carrier == i].bus0.map(n.buses.carrier)[0]
            if n.links[n.links.carrier == i].bus1.map(n.buses.carrier)[0] == "AC":
                df.loc[i, "multiply with efficiency"] = 0
                df.loc[i, "multiply with"] = df.loc[i, "multiply with"] = n.links[n.links.carrier == i].efficiency[0]
            if n.links[n.links.carrier == i].bus1.map(n.buses.carrier)[0] == "low voltage":
                df.loc[i, "multiply with efficiency"] = 0
                df.loc[i, "multiply with"] = n.links[n.links.carrier == i].efficiency[0]
            if df.loc[i, "capacity unit"] == "t/MW_th":
                df.loc[i, "multiply with efficiency"] = 0
                df.loc[i, "multiply with"] = n.links[n.links.carrier == i].efficiency[0]
    chps = ['urban central gas CHP', 'urban central gas CHP CC',
        'urban central solid biomass CHP',
        'urban central solid biomass CHP CC']
    for chp in chps:
        df.loc[chp, "multiply with efficiency"] = "1,2"
        df.loc[chp, "multiply with"] = n.links[n.links.carrier == chp].efficiency[0]+n.links[n.links.carrier == chp].efficiency2[0]

    df["material cost per capacity unit"] = (df[["steel", "cement", "hvc"]]*comm_prices_2020).sum(axis = 1)
    df["an material cost per capacity unit"] = df["material cost per capacity unit"]/annuity(df["lifetime"], 0.07)
    # including local production share (if 0, then 0 is subtracted)
    df["an capital cost without material per p_nom"] = df["an capital cost"] - (
        df["an material cost per capacity unit"]*df["multiply with"]*local_prod_perc)                      
    return df, material_data

def correct_capital_costs(n, df):
    for i in n.links[n.links.carrier.isin(df.index)].index:
        if i in ['offwind-dc', 'offwind-ac', 'onwind', 'solar', 'solar rooftop']:
            n.generators.loc[i, "capital_cost"] = df.loc[n.generators.loc[i, "carrier"], "an capital cost without material per p_nom"]
        elif i in ['H2 Store', 'battery']:
            n.stores.loc[i, "capital_cost"] = df.loc[n.stores.loc[i, "carrier"], "an capital cost without material per p_nom"]
        else:
            n.links.loc[i, "capital_cost"] = df.loc[n.links.loc[i, "carrier"], "an capital cost without material per p_nom"]

############################################################
# fix capacities other countries (gens, links, lines, stores)
# to capacities from solved coupled model without endo. mat.
############################################################
def fix_neighbouring_countries_capacities(n):
    
    namec = f"../post-networks-mat/Mat_{mat_scenario}_H2_{H2_importcost}_local_0.nc" 
    nc = pypsa.Network(namec)

    idx_de = n.generators.filter(like = "DE", axis = 0).index
    gens_not_de = n.generators.drop(idx_de).index
    n.generators.loc[gens_not_de, "p_nom_extendable"] = False
    n.generators.loc[gens_not_de, "p_nom"] = nc.generators.loc[gens_not_de, "p_nom_opt"]

    idx_de = n.links.filter(like = "DE", axis = 0).index
    links_not_de = n.links.drop(idx_de).index
    n.links.loc[links_not_de, "p_nom_extendable"] = False
    n.links.loc[links_not_de, "p_nom"] = nc.links.loc[links_not_de, "p_nom_opt"]
    # alternatively: only dc links
    # dc = n.links[n.links.carrier == "DC"].index
    # n.links.loc[dc, "p_nom_extendable"] = False
    # n.links.loc[dc, "p_nom"] = nc.links.loc[dc, "p_nom_opt"]

    n.lines.loc[:,"s_nom_extendable"] = False
    n.lines.loc[:,"s_nom"] = nc.lines.loc[:,"s_nom_opt"]

    idx_de = n.stores.filter(like = "DE", axis = 0).index
    not_de = n.stores.drop(idx_de).index
    n.stores.loc[not_de, "e_nom_extendable"] = False
    n.stores.loc[not_de, "e_nom"] = nc.stores.loc[not_de, "e_nom_opt"]
def fix_H2_imports(n):
    namec = f"../post-networks-mat/Mat_{mat_scenario}_H2_{H2_importcost}_local_0.nc" 
    nc = pypsa.Network(namec)
    idx = nc.generators[nc.generators.carrier == "H2 import"].index
    n.madd("Bus", names = idx, carrier = "H2 import")
    n.madd("Store", names = idx, 
        bus = idx,
        e_initial = nc.generators_t.p[idx].sum().values,
        e_nom = nc.generators_t.p[idx].sum().values,
        carrier = "H2 import", e_nom_extendable = False, 
        marginal_cost = H2_importcost)
    n.madd("Link", names = idx, bus0 = idx, bus1 = nc.generators.loc[idx, "bus"].values,
           p_min_pu = 0, p_nom_extendable = True)
    n.generators.loc[idx, "p_nom_extendable"] = False
    n.generators.loc[idx, "p_nom"] = 0


#############################################
def solve_industry_module_mat(n, local_prod_perc, df, material_data, co2_cap, snapshots): 
    # add generators for energy system materials
    n.madd("Generator", ["esm steel", "esm cement", "esm hvc"], 
        bus = ["DE steel", "DE cement", "DE hvc"],
        carrier = ["steel", "cement", "hvc"],
        p_nom_extendable = True, 
        p_min_pu = -1, 
        p_max_pu = -1)
    
    # add constraints for material production: material demand is increased by
    # material demand for technology expansion
    n.optimize.create_model()
    def material_constraint(n, snapshots, material):
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
        
        name = f"esm {material}"
        md = material_data[material_data.material == material]["value"]
        offwindgens = n.generators[n.generators.carrier.isin(["offwind-ac", "offwind-dc"])].index
        onwindgens = n.generators[n.generators.carrier == "onwind"].index
        solargens = n.generators[n.generators.carrier.isin(["solar"])].index
        roofsolargens = n.generators[n.generators.carrier.isin(["solar rooftop"])].index
        
        def mat_expr(tech):
            carrier = map_carrier_to_tech[tech]

            if tech in n.links.carrier.unique():
                indices = n.links[n.links.carrier == tech].index
                return [
                    (
                        local_prod_perc *
                        md.loc[carrier] /
                        df.loc[tech, "lifetime"] *
                        df.loc[tech, "multiply with"],
                        get_var(n, "Link", "p_nom").loc[i]
                    )
                    for i in indices
                ]

            elif tech in n.generators.carrier.unique():
                indices = n.generators[n.generators.carrier == tech].index
                return [
                    (
                        local_prod_perc *
                        md.loc[carrier] /
                        lifetime.get(tech, df.loc[tech, "lifetime"]),
                        get_var(n, "Generator", "p_nom").loc[i]
                    )
                    for i in indices
                ]

            elif tech in n.stores.carrier.unique():
                indices = n.stores[n.stores.carrier == tech].index
                return [
                    (
                        local_prod_perc *
                        md.loc[carrier] /
                        df.loc[tech, "lifetime"] *
                        df.loc[tech, "multiply with"],
                        get_var(n, "Store", "e_nom").loc[i]
                    )
                    for i in indices
                ]

            else:
                raise ValueError(f"Unknown technology or carrier: {tech}")


        esm_terms = (
        mat_expr("offwind-dc") +
        mat_expr("onwind") +
        mat_expr("solar") +
        mat_expr("solar rooftop") +

        mat_expr("urban central gas CHP CC") +
        mat_expr("urban central gas CHP") +
        mat_expr("OCGT") +
        mat_expr("H2 turbine") +
        mat_expr("H2 Fuel Cell") +

        mat_expr("urban central solid biomass CHP CC") +
        mat_expr("lowT industry solid biomass CC") +
        mat_expr("solid biomass for highT industry CC") +
        mat_expr("solid biomass for mediumT industry CC") +
        mat_expr("solid biomass to electricity CC") +

        mat_expr("urban central solid biomass CHP") +
        mat_expr("residential rural biomass boiler") +
        mat_expr("services rural biomass boiler") +
        mat_expr("residential urban decentral biomass boiler") +
        mat_expr("services urban decentral biomass boiler") +
        mat_expr("lowT industry solid biomass") +
        mat_expr("solid biomass for mediumT industry") +
        mat_expr("solid biomass to electricity") +
        mat_expr("solid biomass for highT industry") +

        mat_expr("residential rural gas boiler") +
        mat_expr("services rural gas boiler") +
        mat_expr("residential urban decentral gas boiler") +
        mat_expr("services urban decentral gas boiler") +
        mat_expr("urban central gas boiler") +
        mat_expr("lowT industry methane") +
        mat_expr("gas for mediumT industry") +
        mat_expr("gas for highT industry") +

        mat_expr("gas for mediumT industry CC") +
        mat_expr("lowT industry methane CC") +
        mat_expr("gas for highT industry CC") +

        mat_expr("lowT industry heat pump") +
        mat_expr("urban central air heat pump") +
        mat_expr("residential urban decentral air heat pump") +
        mat_expr("residential rural ground heat pump") +
        mat_expr("services rural ground heat pump") +
        mat_expr("services urban decentral air heat pump") +

        mat_expr("DAC") +

        mat_expr("H2 Store") +
        mat_expr("battery") +
        mat_expr("residential rural water tanks") +
        mat_expr("services rural water tanks") +
        mat_expr("residential urban decentral water tanks") +
        mat_expr("services urban decentral water tanks") +
        mat_expr("urban central water tanks") +

        [( - len(n.snapshots), get_var(n, "Generator", "p_nom")[name] )]
        ) 

        esm = linexpr(*esm_terms)
                    
        define_constraints(n, esm, "<=", 0, "Generator", name)

    def EU_material_constraint(n, snapshots, material):
        lifetime = {'dac': 20,
        'nuclear': 60,
        'fossil fuel based generators': 40,
        'other renewables': 30,
        'process heat': 15,
        'offwind-ac': 25,
        'offwind-dc': 25,
        'onwind': 25,
        'solar': 25,
        'solar thermal': 25,
        'CSP': 25}   


        md = material_data[material_data.material == material]["value"]
        name = f"esm {material}"
        offwindgens = n.generators[n.generators.carrier.isin(["offwind-ac", "offwind-dc"])].index
        onwindgens = n.generators[n.generators.carrier == "onwind"].index
        solargens = n.generators[n.generators.carrier.isin(["solar"])].index
        roofsolargens = n.generators[n.generators.carrier.isin(["solar rooftop"])].index
        esm = (
        # renewables
        (local_prod_perc*md.loc["offwind"]/lifetime["offwind-dc"]*
         get_var(n, "Generator", "p_nom").loc[offwindgens].sum()) +
        (local_prod_perc*md.loc["onwind"]/lifetime["onwind"]*
        get_var(n, "Generator", "p_nom").loc[onwindgens].sum()) +
        (local_prod_perc*md.loc["solar"]/lifetime["solar"]*
         get_var(n, "Generator", "p_nom").loc[solargens].sum()) +
        (local_prod_perc*md.loc["solar rooftop"]/lifetime["solar"]*
        get_var(n, "Generator", "p_nom").loc[roofsolargens].sum()) + 
        (-8760*get_var(n, "Generator", "p_nom").loc[name])
        )
        define_constraints(n, esm, "<=", 0, "Generator", name)

    # co2 constraint
    def co2_constraint(n, snapshots):
        sus_others_co2 = linexpr((1, get_var(n, "Store", "e")[snapshots[-1], "co2 atmosphere"]))
        define_constraints(n, sus_others_co2, "<=", co2_cap, "Store", "others co2 cap")

    def co2_de_constraint(n, snapshots):
        sus_de_co2 = linexpr((1, get_var(n, "Store", "e")[snapshots[-1], "DE co2 atmosphere"]))
        define_constraints(n, sus_de_co2, "<=", co2_cap, "Store", "DE co2 cap")

    def extra_functionalities(n, snapshots):
        for material in ["steel", "cement", "hvc"]:
            material_constraint(n, snapshots, material)
        co2_constraint(n, snapshots)
        co2_de_constraint(n, snapshots)
    
    n.optimize(solver_name="gurobi", snapshots = n.snapshots, 
            extra_functionality = extra_functionalities,
            solver_options= {"threads": 4, "method": 2, "crossover": 0,
            "BarConvTol": 1e-6,
            "Seed": 123,
            "AggFill": 0,
            "PreDual": 0,
            "GURO_PAR_BARDENSETHRESH": 200})
    return n

