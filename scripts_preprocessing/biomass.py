import pandas as pd
import pypsa as pypsa
###########################
# input
biomass_scenario = "ENS_Med"
year = 2040

##########################
# adapted from pypsa-eur method
def annuity(lifetime, dr):
    return (1-(1+dr)**-lifetime)/dr
def prepare_costs(cost_file, USD_to_EUR, discount_rate, Nyears, lifetime):
    # set all asset costs and other parameters
    costs = pd.read_csv(cost_file, index_col=[0, 1]).sort_index()
    
    # correct units to MW and EUR
    costs.loc[costs.unit.str.contains("/kW"), "value"] *= 1e3
    costs.loc[costs.unit.str.contains("USD"), "value"] *= USD_to_EUR

    # min_count=1 is important to generate NaNs which are then filled by fillna
    costs = costs.loc[:, "value"].unstack(level=1).groupby("technology").sum(min_count=1)
    costs = costs.fillna({"CO2 intensity": 0,
                          "FOM": 0,
                          "VOM": 0,
                          "discount rate": discount_rate,
                          "efficiency": 1,
                          "fuel": 0,
                          "investment": 0,
                          "lifetime": lifetime
                          })

    annuity_factor = lambda v: annuity(v["lifetime"], v["discount rate"]) + v["FOM"] / 100
    costs["fixed"] = [1/annuity_factor(v) * v["investment"] * Nyears for i, v in costs.iterrows()]

    return costs

def enspreso_biomass_potentials(year, scenario):
    """
    Loads the JRC ENSPRESO biomass potentials.

    Parameters
    ----------
    year : int
        The year for which potentials are to be taken.
        Can be {2010, 2020, 2030, 2040, 2050}.
    scenario : str
        The scenario. Can be {"ENS_Low", "ENS_Med", "ENS_High"}.

    Returns
    -------
    pd.DataFrame
        Biomass potentials for given year and scenario
        in TWh/a by commodity and NUTS2 region.
    """
    filename =  "../data/raw/ENSPRESO_BIOMASS.xlsx"
    glossary = pd.read_excel(
        filename,
        sheet_name="Glossary",
        usecols="B:D",
        skiprows=1,
        index_col=0
    )

    df = pd.read_excel(
        filename,
        sheet_name="ENER - NUTS2 BioCom E",
        usecols="A:H"
    )

    df2 = pd.read_excel(
        filename,
        sheet_name="COST - NUTS2 BioCom",
        usecols="A:H"
    )

    df["group"] = df["E-Comm"].map(glossary.group)
    df["commodity"] = df["E-Comm"].map(glossary.description)

    to_rename = {
        "NUTS2 Potential available by Bio Commodity": "potential",
        "NUST2": "NUTS2",
    }
    df.rename(columns=to_rename, inplace=True)

    # fill up with NUTS0 if NUTS2 is not given
    df.NUTS2 = df.apply(lambda x: x.NUTS0 if x.NUTS2 == '-' else x.NUTS2, axis=1)

    # convert PJ to TWh
    df.potential *= 1e6/3.6
    df.Unit = "MWh/a"

    year = year
    scenario = scenario
    dff = df.query("Year == @year and Scenario == @scenario")

    bio = dff.groupby(["NUTS2", "commodity"]).potential.sum().unstack()

    # currently Serbia and Kosovo not split, so aggregate
    bio.loc["RS"] += bio.loc["XK"]
    bio.drop("XK", inplace=True)

    #====
    
    to_rename = {
         "NUTS2 Bio Commodity Cost ": "cost",
        "Energy commoditty ": "Energy commodity",
    }
    df2.rename(columns=to_rename, inplace=True)

    df2["group"] = df2["Energy commodity"].map(glossary.group)
    df2["commodity"] = df2["Energy commodity"].map(glossary.description)
    
    # fill up with NUTS0 if NUTS2 is not given
    df2.NUTS2 = df2.apply(lambda x: x.NUTS0 if x.NUTS2 == '-' else x.NUTS2, axis=1)

    # convert €/GJ to €/MWh
    df2.cost *= 3.6
    df2.Units = "€/MWh"

    year = year
    scenario = scenario

    dff2 = df2.query("Year == @year and Scenario == @scenario")

    bio2 = dff2.groupby(["NUTS2", "commodity"]).cost.sum().unstack()

    data = {
    'Manure solid, liquid': 'manureslurry',
    'Municipal waste': 'municipal solid waste',
    'Sludge': 'sewage sludge',
    'Agricultural waste': 'straw',
    'Fuelwood residues': 'forest residues',
    'Residues from landscape care': 'landscape care',
    'Secondary Forestry residues - woodchips': 'industry wood residues',
    'Sawdust': 'industry wood residues',
    'Sugar from sugar beet': 'not included',
    'Rape seed': 'not included',
    'Sunflower, soya seed ': 'not included',
    'Bioethanol barley, wheat, grain maize, oats, other cereals and rye': 'not included',
    'Miscanthus, switchgrass, RCG': 'not included',
    'Willow': 'not included',
    'Poplar': 'not included',
    'FuelwoodRW': 'not included',
    'C&P_RW': 'not included'
    }

    bio.loc["class"] = 0
    for c in bio.columns:
        bio.loc["class", c] = data[c]

    bio2.loc["class"] = 0
    for c in bio2.columns:
        bio2.loc["class", c] = data[c]
    
    bio = bio.T.groupby("class").sum()
    bio2 = bio2.T.groupby("class").mean()

    return bio.T, bio2.T

def aggregate_biomass_de(bio):
    return bio.filter(like = "DE", axis = 0).fillna(0).sum(axis = 0)
def average_biomass_de(bio):
    return bio.filter(like = "DE", axis = 0).fillna(0).mean(axis = 0)


def make_biomass_costs():
    bio = pd.read_csv("biomass_potentials.csv", index_col = 0)
    biocost = pd.read_csv("biomass_cost.csv", index_col = 0)
    # enspreso_biomass_potentials(year=2040, scenario="ENS_Med")
    bio_de = aggregate_biomass_de(bio)
    biocost_de = average_biomass_de(biocost)
    costs = pd.read_csv("prepared_costs.csv", index_col = 0)
    biomass_costs_node = biocost_de
    biomass_pot_node = bio_de
    biomass_types =  ["manureslurry", "sewage sludge", "straw", "forest residues", "industry wood residues", "landscape care", "municipal solid waste"]
    biomass_potential = {}
    biomass_costs = {}
    for name in biomass_types:
        biomass_potential[name] = biomass_pot_node[name]
        if name in biomass_costs_node.index:
            biomass_costs[name] = biomass_costs_node[name]
        else:
            biomass_costs[name] = 0 #TODO
    digestible_biomass_types = ["manureslurry", "sewage sludge", "straw"]
    name = "digestible biomass"
    biomass_potential[name] = sum(biomass_potential[types] for types in digestible_biomass_types)
    biomass_costs[name] = sum(biomass_costs[types] for types in digestible_biomass_types)
    solid_biomass_types = ["forest residues", "industry wood residues", "landscape care"]
    name = "solid biomass"
    biomass_potential[name] = sum(biomass_potential[types] for types in solid_biomass_types)
    biomass_costs[name] = sum(biomass_costs[types] for types in solid_biomass_types)
    return biomass_costs, biomass_potential

def add_biomass(n):
    '''
    solid biomass not added because already in model
    '''
    beccs = True
    bio = pd.read_csv("biomass_potentials.csv", index_col = 0)
    biocost = pd.read_csv("biomass_cost.csv", index_col = 0)
    # enspreso_biomass_potentials(year=2040, scenario="ENS_Med")
    bio_de = aggregate_biomass_de(bio)
    biocost_de = average_biomass_de(biocost)
    costs = pd.read_csv("prepared_costs.csv", index_col = 0)

    # prepare_costs("costs_2045.csv", USD_to_EUR=0.7532,  discount_rate = 0.07, Nyears = 1, lifetime = 25)
    nodes = "DE"

    # biomass distributed at country level - i.e. transport within country allowed
    # cts = pop_layout.ct.value_counts().index

    biomass_pot_node = bio_de

    # need to aggregate potentials if gas not nodally resolved
    # if options["gas_network"]:
    #     biogas_potentials_spatial = biomass_potentials["biogas"].rename(index=lambda x: x + " biogas")
    # else:
    #     biogas_potentials_spatial = biomass_potentials["biogas"].sum()

    # if options["biomass_transport"]:
    #     solid_biomass_potentials_spatial = biomass_potentials["solid biomass"].rename(index=lambda x: x + " solid biomass")
    # else:
    #     solid_biomass_potentials_spatial = biomass_potentials["solid biomass"].sum()

    # potential per node distributed within country by population
    # biomass_pot_node = (biomass_potentials.loc[pop_layout.ct]
    #                     .set_index(pop_layout.index)
    #                     .mul(pop_layout.fraction, axis="index"))

    biomass_costs_node = biocost_de
    # biomass_costs_node = biomass_costs
    # print(biomass_costs_node)
    # print(biomass_pot_node)

    # Digestible biomass
    n.add("Carrier", "digestible biomass")

    n.add("Bus",
           nodes + " digestible biomass",
           location=nodes,
           carrier="digestible biomass")

    '''n.add("Store",
           nodes + " digestible biomass",
           bus=nodes + " digestible biomass",
           carrier="digestible biomass",
           e_cyclic=True)'''

    # Municipal solid waste
    n.add("Carrier", "municipal solid waste")

    n.add("Bus",
           nodes + " municipal solid waste",
           location=nodes,
           carrier="municipal solid waste")

    '''n.add("Store",
           nodes + " municipal solid waste",
           bus=nodes + " municipal solid waste",
           carrier="municipal solid waste",
           e_cyclic=True)'''

    # Solid biomass
    '''n.add("Carrier", "solid biomass")

    n.add("Bus",
           nodes + " solid biomass",
           location=nodes,
           carrier="solid biomass")'''

    '''n.add("Store",
           nodes + " solid biomass",
           bus=nodes + " solid biomass",
           carrier="solid biomass",
           e_cyclic=True)'''


    biomass_types =  ["manureslurry", "sewage sludge", "straw", "forest residues", "industry wood residues", "landscape care", "municipal solid waste"]
    biomass_potential = {}
    biomass_costs = {}
    for name in biomass_types:
        biomass_potential[name] = biomass_pot_node[name]
        if name in biomass_costs_node.index:
            biomass_costs[name] = biomass_costs_node[name]
        else:
            biomass_costs[name] = 0 #TODO

    print('municipal solid waste cost: ',biomass_costs['municipal solid waste'])
    n.add("Store",
           nodes + " municipal solid waste",
           bus=nodes + " municipal solid waste",
           carrier="municipal solid waste",
           e_nom=biomass_potential['municipal solid waste'],
           e_nom_max=biomass_potential['municipal solid waste'],
           e_initial=biomass_potential['municipal solid waste'],
           e_nom_extendable = False,
           marginal_cost=biomass_costs['municipal solid waste'])

    digestible_biomass_types = ["manureslurry", "sewage sludge", "straw"]
    name = "digestible biomass"
    biomass_potential[name] = sum(biomass_potential[types] for types in digestible_biomass_types)
    biomass_costs[name] = sum(biomass_costs[types] for types in digestible_biomass_types)
    print(name,' cost: ',biomass_costs[name])

    n.add("Store",
            nodes + " digestible biomass",
            bus=nodes + " digestible biomass",
            carrier=name,
            e_nom_extendable=False,
            e_nom=biomass_potential[name],
            e_initial=biomass_potential[name],
            e_nom_max=biomass_potential['municipal solid waste'],
            marginal_cost=biomass_costs[name])

    # TODO: gas grid cost added for biogas processes in insert_gas_distribution_costs, but demands refining! Also add CO2 transport cost!
    n.add("Link",
           nodes + " biogas",
           bus0=nodes + " digestible biomass",
           bus1="DE gas",
           bus3="DE co2 atmosphere",
           carrier="biogas",
           capital_cost=(costs.at["biogas", "FOM"] + costs.at["biogas upgrading", "FOM"]) * costs.at["biogas","efficiency"],
           marginal_cost=costs.at["biogas upgrading", "VOM"] * costs.at["biogas","efficiency"],
           efficiency=costs.at["biogas","efficiency"],
           efficiency3=-costs.at['gas', 'CO2 intensity'] * costs.at["biogas","efficiency"],
           p_nom_extendable=True)

    if beccs:
        n.add("Link",
               nodes + " biogas CC",
               bus0=nodes + " digestible biomass",
               bus1="DE gas",
               bus2="DE co2 stored",
               bus3="DE co2 atmosphere",
               carrier="biogas CC",
               capital_cost=(costs.at["biogas CC", "FOM"] + costs.at["biogas upgrading", "FOM"]) * costs.at["biogas CC", "efficiency"]
                            + costs.at['biomass CHP capture', 'fixed'] * costs.at["biogas CC", "CO2 stored"],
               # Assuming that the CO2 from upgrading is pure, such as in amine scrubbing. I.e., with and without CC is
               # equivalent. Adding biomass CHP capture because biogas is often small-scale and decentral so further
               # from e.g. CO2 grid or buyers. This is a proxy for the added cost for e.g. a raw biogas pipeline to a central upgrading facility
               marginal_cost=(costs.at["biogas CC", "VOM"] + costs.at["biogas upgrading", "VOM"]) * costs.at["biogas","efficiency"],
               efficiency=costs.at["biogas CC", "efficiency"],
               efficiency2=costs.at["biogas CC", "CO2 stored"] * 0.9,
               efficiency3=-costs.at['gas', 'CO2 intensity'] * costs.at["biogas CC", "efficiency"] - costs.at["biogas CC", "CO2 stored"] * 0.9,
               p_nom_extendable=True)


    solid_biomass_types = ["forest residues", "industry wood residues", "landscape care"]
    name = "solid biomass"
    biomass_potential[name] = sum(biomass_potential[types] for types in solid_biomass_types)
    biomass_costs[name] = sum(biomass_costs[types] for types in solid_biomass_types)
  
    # #Update solid biomass costs according to sensitivity settings
    # if 'BM0' in snakemake.wildcards.bm_s:
    #     pass
    # elif 'BM2' in snakemake.wildcards.bm_s:
    #     biomass_costs[name] = biomass_costs[name] + (biomass_import_price - biomass_costs[name]) / 3
    # elif 'BM3' in snakemake.wildcards.bm_s:
    #     biomass_costs[name] = biomass_costs[name] + (biomass_import_price - biomass_costs[name]) * 2 / 3
    # elif 'BM4' in snakemake.wildcards.bm_s:
    #     biomass_costs[name] = biomass_import_price
    # elif 'BM1' in snakemake.wildcards.bm_s:
    #     pass

    print(name, ' cost: ', biomass_costs["solid biomass"])

    n.stores.loc[nodes + " solid biomass", "e_nom"] = biomass_potential["solid biomass"]
    n.stores.loc[nodes + " solid biomass", "e_initial"] = biomass_potential["solid biomass"]
    n.stores.loc[nodes + " solid biomass", "marginal_cost"] = biomass_costs["solid biomass"]

    '''n.add("Store",
            nodes + " solid biomass",
            bus=nodes + " solid biomass",
            carrier=name + " solid biomass",
            e_nom_extendable=False,
            e_nom=biomass_potential["solid biomass"],
            e_initial=biomass_potential["solid biomass"],
            e_nom_max=biomass_potential['municipal solid waste'],
            marginal_cost=biomass_costs["solid biomass"])'''

    n.add("Link",
           nodes + " solid biomass to gas",
           bus0=nodes + " solid biomass",
           bus1="DE gas",
           bus3="DE co2 atmosphere",
           carrier="BioSNG",
           lifetime=costs.at['BioSNG', 'lifetime'],
           efficiency=costs.at['BioSNG', 'efficiency'],
           efficiency3=-costs.at['solid biomass', 'CO2 intensity'] + costs.at['BioSNG', 'CO2 stored'],
           p_nom_extendable=True,
           capital_cost=costs.at['BioSNG', 'fixed'] * costs.at['BioSNG', 'efficiency'],
           marginal_cost=costs.at['BioSNG', 'efficiency']*costs.loc["BioSNG", "VOM"]
           )

    if beccs:
        n.add("Link",
               nodes + " solid biomass to gas CC",
               bus0=nodes + " solid biomass",
               bus1="DE gas",
               bus2="DE co2 stored",
               bus3="DE co2 atmosphere",
               carrier="BioSNG CC",
               lifetime=costs.at['BioSNG', 'lifetime'],
               efficiency=costs.at['BioSNG', 'efficiency'],
               efficiency2=costs.at['BioSNG', 'CO2 stored'] * 0.9,
               efficiency3=-costs.at['solid biomass', 'CO2 intensity'] + costs.at['BioSNG', 'CO2 stored'] * (1 - 0.9),
               p_nom_extendable=True,
               capital_cost=costs.at['BioSNG', 'fixed'] * costs.at['BioSNG', 'efficiency'] + costs.at['biomass CHP capture', 'fixed'] * costs.at[
                   "BioSNG", "CO2 stored"],
               marginal_cost=costs.at['BioSNG', 'efficiency']*costs.loc["BioSNG", "VOM"]
               )

        '''n.add("Link",
               nodes + " solid biomass to hydrogen CC",
               bus0=nodes + " solid biomass",
               bus1=nodes + " H2",
               bus2="DE co2 stored",
               bus3="DE co2 atmosphere",
               carrier="solid biomass to hydrogen CC",
               efficiency=costs.at['solid biomass to hydrogen', 'efficiency'],
               efficiency2=costs.at['solid biomass', 'CO2 intensity'] * 0.9,
               efficiency3=-costs.at['solid biomass', 'CO2 intensity'] + costs.at['solid biomass', 'CO2 intensity'] * (1 - 0.9),
               p_nom_extendable=True,
               capital_cost=costs.at['solid biomass to hydrogen', 'fixed'] * costs.at['solid biomass to hydrogen', 'efficiency']
                            + costs.at['biomass CHP capture', 'fixed'] * costs.at['solid biomass', 'CO2 intensity'],
               marginal_cost=0.,
               )'''

    n.add("Link",
           nodes + " biomass to liquid",
           bus0=nodes + " solid biomass",
           bus1=nodes+" oil",
           bus3="DE co2 atmosphere",
           carrier="biomass to liquid",
           lifetime=costs.at['BtL', 'lifetime'],
           efficiency=costs.at['BtL', 'efficiency'],
           efficiency3=-costs.at['solid biomass', 'CO2 intensity'] + costs.at['BtL', 'CO2 stored'],
           p_nom_extendable=True,
           capital_cost=costs.at['BtL', 'fixed'] * costs.at['BtL', 'efficiency'],
           marginal_cost=costs.at['BtL', 'efficiency']*costs.loc["BtL", "VOM"]
           )

    if beccs:
        #Assuming that acid gas removal (incl. CO2) from syngas i performed with Rectisol process (Methanol) and electricity demand for this is included in the base process
        n.add("Link",
               nodes + " biomass to liquid CC",
               bus0=nodes + " solid biomass",
               bus1=nodes+" oil",
               bus2="DE co2 stored",
               bus3="DE co2 atmosphere",
               carrier="biomass to liquid CC",
               lifetime=costs.at['BtL', 'lifetime'],
               efficiency=costs.at['BtL', 'efficiency'],
               efficiency2=costs.at['BtL', 'CO2 stored'] * 0.9,
               efficiency3=-costs.at['solid biomass', 'CO2 intensity'] + costs.at['BtL', 'CO2 stored'] * (1 - 0.9),
               p_nom_extendable=True,
               capital_cost=costs.at['BtL', 'fixed'] * costs.at['BtL', 'efficiency'] + costs.at['biomass CHP capture', 'fixed'] * costs.at[
                   "BtL", "CO2 stored"],
               marginal_cost=costs.at['BtL', 'efficiency'] * costs.loc["BtL", "VOM"]
               )
    '''print('Adding electrobiofuels')
    efuel_scale_factor = costs.at['BtL', 'C stored']
    n.add("Link",
            nodes + " electrobiofuels",
            bus0=nodes + " solid biomass",
            bus1=nodes+" oil",
            bus5=nodes + " H2",
            bus3="DE co2 atmosphere",
            carrier="electrobiofuels",
            lifetime=costs.at['electrobiofuels', 'lifetime'],
            efficiency=costs.at['electrobiofuels', 'efficiency-biomass'],
            efficiency5=-costs.at['electrobiofuels', 'efficiency-hydrogen'],
            efficiency3=-costs.at['solid biomass', 'CO2 intensity'] + costs.at['BtL', 'CO2 stored'] * (1 - costs.at['Fischer-Tropsch', 'capture rate']),
            p_nom_extendable=True,
            capital_cost=costs.at['BtL', 'fixed'] * costs.at['electrobiofuels', 'efficiency-biomass'] \
                        + efuel_scale_factor * costs.at['Fischer-Tropsch', 'fixed'] * costs.at['electrobiofuels', 'efficiency-hydrogen'],
            marginal_cost=costs.at['BtL', 'VOM'] * costs.at['electrobiofuels', 'efficiency-biomass'] \
                            + efuel_scale_factor * costs.at['Fischer-Tropsch', 'VOM'] * costs.at['electrobiofuels', 'efficiency-hydrogen']
            )

    
    # TODO: Add real data for bioelectricity without CHP!
    n.add("Link",
           nodes + " solid biomass to electricity",
           bus0=nodes + " solid biomass",
           bus1=nodes,
           bus3="DE co2 atmosphere",
           carrier="solid biomass to electricity",
           p_nom_extendable=True,
           capital_cost=0.7 * costs.at['central solid biomass CHP', 'fixed'] * costs.at[
               'central solid biomass CHP', 'efficiency'],
           marginal_cost=costs.at['central solid biomass CHP', 'VOM'],
           efficiency=1.3 * costs.at['central solid biomass CHP', 'efficiency'],
           efficiency3=costs.at['solid biomass', 'CO2 intensity']-costs.at['solid biomass', 'CO2 intensity'],
           lifetime=costs.at['central solid biomass CHP', 'lifetime'])

    if beccs:
        n.add("Link",
               nodes + " solid biomass to electricity CC",
               bus0=nodes + " solid biomass",
               bus1=nodes,
               bus2="DE co2 stored",
               bus3="DE co2 atmosphere",
               carrier="solid biomass to electricity CC",
               p_nom_extendable=True,
               capital_cost=0.7 * costs.at['central solid biomass CHP CC', 'fixed'] * costs.at[
                   'central solid biomass CHP CC', 'efficiency']
                            + costs.at['biomass CHP capture', 'fixed'] * costs.at['solid biomass', 'CO2 intensity'],
               marginal_cost=costs.at['central solid biomass CHP CC', 'VOM'],
               efficiency=1.3 * costs.at['central solid biomass CHP CC', 'efficiency'],
               efficiency2=costs.at['solid biomass', 'CO2 intensity'] * 0.9,
               efficiency3=-costs.at['solid biomass', 'CO2 intensity'] + costs.at['solid biomass', 'CO2 intensity'] * (1 - 0.9),
               # p_nom_ratio=costs.at['central solid biomass CHP', 'p_nom_ratio'],
               lifetime=costs.at['central solid biomass CHP CC', 'lifetime'])
    '''
    n.stores.e_min_pu = 0
    
    return n

bio, biocost = enspreso_biomass_potentials(year=2040, scenario=biomass_scenario)
bio.to_csv(f"../data/biomass_potentials_DE_{year}_{biomass_scenario}.csv")
biocost.to_csv(f"../data/biomass_cost_DE_{year}_{biomass_scenario}.csv")
