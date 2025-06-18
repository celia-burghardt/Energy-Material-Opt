import pypsa as pypsa
import pandas as pd
import numpy as numpy
import geopandas as gpd

def industry_module(n, steel_load, hvc_load, cement_load, methanol_load,
    steel_energy, steel_feedstock, steel_cost,
    hvc_energy, hvc_feedstock, hvc_cost,
    cement_energy, cement_feedstock, cement_cost,                       
    max_waste, max_packaging_waste, max_scrap, 
    sectors = ["cement", "hvc", "steel"], 
    ): 
   
    ############################################################
    # choose AC nodes to attach industry links to
    ############################################################
    nodes = n.buses[n.buses.carrier == "AC"].filter(like = "DE", axis = 0).index

    ############################################################
    # Add steel: carrier, bus, links (processes), loads, 
    # + steel scrap: carrier, bus, store
    ############################################################
    n.add("Carrier",
            name="steel")

    n.add("Bus",
            name="DE steel",
            carrier="steel")

    n.add("Load",
            name="DE steel demand",
            bus="DE steel",
            p_set=steel_load*1e3/8760, # kt -> t, 1yr -> 1h
            carrier="steel")

    for p in steel_energy.columns:
        n.add("Carrier",
            name="steel "+p)
        
        for node in nodes:
            n.add("Link",
                name=node+" steel "+p,
                p_nom_extendable=True,
                p_min_pu=1,
                p_max_pu=1,
                carrier="steel "+p,
                bus0=node,
                bus1="DE steel",
                efficiency=1/steel_energy.loc["elec", p],  # bus1/bus0, steel per elec
                bus2=node+" process emissions",
                efficiency2=steel_energy.loc["process emission", p]/steel_energy.loc["elec", p],
                bus3="DE gas for industry",
                efficiency3=-steel_energy.loc["methane", p]/steel_energy.loc["elec", p],
                bus4=node+" H2",
                efficiency4=-steel_energy.loc["hydrogen", p]/steel_energy.loc["elec", p],
                bus5="DE steel scrap",
                efficiency5=-steel_feedstock.loc["steel scrap", p]/steel_energy.loc["elec", p],
                bus6=node+" highT industry",
                efficiency6=-steel_energy.loc["furnaces heat", p]/steel_energy.loc["elec", p],
                marginal_cost=steel_energy.loc["mc_per_t", p]/steel_energy.loc["elec", p],
                capital_cost=steel_energy.loc["an_inv_per_yt", p]/steel_energy.loc["elec", p])

    n.add("Carrier",
            name="steel scrap")

    n.add("Bus",
        name="DE steel scrap",
        carrier="steel scrap")

    n.add("Store",
            name="DE steel scrap",
            bus="DE steel scrap",
            e_initial=max_scrap*1e3,
            e_nom=max_scrap*1e3,
            e_nom_extendable=False)

    ############################################################
    # Add hvc: carrier, bus, links (processes), loads
    # + plastic waste: carrier, bus, store
    # + other basic chemicals: loads
    ############################################################
    
    n.add("Carrier", name = "hvc")  
    n.add("Bus", "DE hvc", carrier = "hvc")        
    n.add("Load", name = "DE hvc demand", bus = "DE hvc", 
        p_set = hvc_load*1e3/8760, # kt -> t, 1yr -> 1h
        carrier = "hvc")
                
    for p in hvc_energy.loc[:,:"MtO"].columns:
        n.add("Carrier", name = "hvc "+p) 
        for node in nodes:
            n.add("Link", node+" hvc "+p,
                    p_nom_extendable = True,
                    p_min_pu = 1, p_max_pu= 1, 
                    carrier = "hvc "+p,
                    bus0 = node,
                    bus1 = "DE hvc",
                    efficiency = (1/hvc_energy.loc["elec", p]), # bus1/bus0, steel per elec
                    bus2 = "DE oil for industry", 
                    efficiency2 = -hvc_energy.loc["naphtha", p]/hvc_energy.loc["elec", p], # bus2/bus0, H2 per elec #+sign: bus0 -> bus1
                    bus3 = "DE methanol for industry",  
                    efficiency3 = (-hvc_energy.loc["methanol", p]/hvc_energy.loc["elec", p]), # bus2/bus0, H2 per elec #+sign: bus0 -> bus1
                    #bus4 = node+" process emissions", 
                    #efficiency4 = (hvc_energy.loc["process emission", p]/hvc_energy.loc["elec", p]),
                    bus5 = "DE gas for industry", 
                    efficiency5 = (- hvc_energy.loc["methane", p]/hvc_energy.loc["elec", p]),
                    bus6 = "DE plastic waste",
                    efficiency6 = -hvc_feedstock.loc["plastic waste", p]/hvc_energy.loc["elec", p],                           
                    marginal_cost = hvc_energy.loc["mc_per_t", p]/hvc_energy.loc["elec", p],
                    capital_cost = hvc_energy.loc["an_inv_per_yt", p]/hvc_energy.loc["elec", p])

    p = "mechanical recycling"
    idx = n.links[n.links.carrier == "hvc mechanical recycling"].index
    n.links.loc[idx, "bus2"] = "DE light packaging waste"
    n.links.loc[idx, "efficiency2"] = -hvc_feedstock.loc["lightweight plastic waste", p]/hvc_energy.loc["elec", p]

    # Add plastic waste buses and links (hvc recycling)
    # light packaging waste is sorted in mechanical recycling and the 
    # leftover waste is sent to plastic waste 
    # (therefore plastic waste store is extendable)
    n.madd("Carrier", names=["plastic waste", "light packaging waste"])

    n.madd("Bus", 
            names=["DE plastic waste", "DE light packaging waste"],
            carrier=["plastic waste", "light packaging waste"])

    n.madd("Store",
            names=["DE plastic waste", "DE light packaging waste"],
            bus=["DE plastic waste", "DE light packaging waste"],
            e_initial=[max_waste*1e3, max_packaging_waste*1e3],
            e_nom=[max_waste*1e3, max_packaging_waste*1e3],
            e_nom_extendable=[True, False],
            e_cyclic = False)

    n.add("Link",
        name="DE packaging waste",
        bus0="DE light packaging waste",
        bus1="DE plastic waste", 
        p_min_pu=0,
        p_nom_extendable=True)

    # basic chemicals heat demand
    steam = hvc_energy.loc["lowT process heat": "highT process heat", "Basic chemicals process heat"]*hvc_energy.loc["production (kt)", "Basic chemicals process heat"]
    n.add("Load", "DE Basic chemicals highT steam", bus = "DE1 0 highT industry", p_set = steam.loc["highT process heat"]/8760)
    n.add("Load", "DE Basic chemicals mediumT steam", bus = "DE1 0 mediumT industry", p_set = steam.loc["mediumT process heat"]/8760)
    n.add("Load", "DE Basic chemicals lowT steam", bus = "DE1 0 lowT industry", p_set = steam.loc["lowT process heat"]/8760)
    n.add("Load", "DE methanol load", bus = "DE methanol for industry", 
          carrier ="methanol load", p_set = methanol_load) 
    
    ############################################################
    # Add cement: carrier, bus, links (processes), loads
    ############################################################
    n.add("Carrier", name="cement")  
    n.add("Bus", name = "DE cement", carrier="cement")
    
    n.add("Load", name = "DE cement demand", 
        bus = "DE cement", 
        p_set = cement_load*1e3/8760, # kt -> t, 1yr -> 1h
        carrier = "cement")

    for p in cement_energy.loc[:,"CEM I":].columns: 
        n.add("Carrier", name= "cement "+p) 
        
        for node in nodes:
            n.add("Link", node+" cement "+p,
                p_nom_extendable = True,
                p_min_pu = 1, p_max_pu= 1, 
                carrier = "cement "+ p,
                bus0 = node,
                bus1 = "DE cement", 
                efficiency = 1/cement_energy.loc["elec", p], #bus1/bus0, steel per elec
                bus2 = node + " highT industry", 
                efficiency2 = - cement_energy.loc[["furnaces heat", "process heat"], p].sum()/cement_energy.loc["elec", p],
                # since only very little process heat, assumed to be all highT for simplicity
                bus3 = node+" process emissions", 
                efficiency3 = cement_energy.loc["process emission", p]/cement_energy.loc["elec", p],
                marginal_cost = cement_energy.loc["mc_per_t", p]/cement_energy.loc["elec", p], 
                capital_cost = cement_energy.loc["an_inv_per_yt", p]/cement_energy.loc["elec", p]) 
    
    # for overnight, remove CEM III/A links (no BF slag available)        
    idx = n.links.filter(like = 'CEM III/A', axis = 0).index   
    n.mremove("Link", idx) 
    return n

def add_process_heat(n):
    #################################################
    # Add more techs for process heat / heat store
    #################################################
    nodeslist = n.buses[n.buses.carrier == "AC"].filter(like = "DE", axis = 0).index
    must_run = 0.8 
    # costs = pd.read_csv("biomass/prepared_costs_2045.csv", index_col = 0)
    costs = pd.read_csv("../data/pypsa-eur/costs_2045.csv", index_col = [0,1], delimiter = ";")["value"]

    for node in nodeslist:
        print("add electricity for mediumT, capital_cost = 85000 (Projektionsbericht 2023, p.181), efficiency = 1 (Agora 2024)")   
        n.add("Link",
                node + " electricity for mediumT industry",
                bus0=node,
                bus1=node + " mediumT industry",
                carrier="mediumT industry electricity",
                p_nom_extendable=True,
                p_min_pu=must_run,
                efficiency=1,
                capital_cost = 85000, # Projektionsbericht 2023, p.181 
                #costs.at["electric boiler steam", "investment"]* costs.at["electric boiler steam", "efficiency"],
                marginal_cost=costs.at["electric boiler steam", "VOM"],
                lifetime=costs.at["electric boiler steam", "lifetime"],
            )
        
        print("add plasma electricity for highT, capital_cost = 200000*(0.5+0.9)/2 (Projektionsbericht 2023, p.181), efficiency = (0.5+0.9)/2 (Agora 2024)")
        n.add(
            "Link",
            node+" plasma for highT industry",
            bus0=node,
            bus1=node + " highT industry", #can go up to 5000 °C
            carrier="plasma for highT industry",
            p_nom_extendable=True,
            p_min_pu=must_run,
            efficiency=(0.5+0.9)/2, # average of 0.5 and 0.9, Agora 2024
            capital_cost= 200000*(0.5+0.9)/2, # Projektionsbericht
            marginal_cost=costs.at["direct firing solid fuels", "VOM"], # 
            lifetime=20, 
        )
        
        n.add(
            "Link",
            node+" solid biomass for highT industry",
            bus0="DE solid biomass",
            bus1=node + " highT industry",
            bus2="DE co2 atmosphere",
            carrier="solid biomass for highT industry",
            p_nom_extendable=True,
            p_min_pu=must_run,
            efficiency=costs.at["direct firing solid fuels", "efficiency"],
            efficiency2=costs.at["solid biomass", "CO2 intensity"]
            - costs.at["solid biomass", "CO2 intensity"],
            capital_cost=costs.at["direct firing solid fuels", "investment"]
            * costs.at["direct firing solid fuels", "efficiency"],
            marginal_cost=costs.at["direct firing solid fuels", "VOM"]
            + costs.at["biomass boiler", "pelletizing cost"],
            lifetime=costs.at["direct firing solid fuels", "lifetime"],
        )

        n.add(
            "Link",
            node+" solid biomass for highT industry CC",
            bus0="DE solid biomass",
            bus1=node + " highT industry",
            bus2="DE co2 stored",
            bus3="DE co2 atmosphere",
            carrier="solid biomass for highT industry CC",
            p_nom_extendable=True,
            p_min_pu=must_run,
            efficiency=costs.at["direct firing solid fuels", "efficiency"],
            capital_cost=costs.at["solid biomass boiler steam CC", "investment"]
            * costs.at["solid biomass boiler steam CC", "efficiency"]
            + costs.at["biomass CHP capture", "investment"]
            * costs.at["solid biomass", "CO2 intensity"],
            marginal_cost=costs.at["solid biomass boiler steam CC", "VOM"],
            efficiency3=costs.at["solid biomass", "CO2 intensity"]
            * (1 - costs.at["biomass CHP capture", "capture_rate"])
            - costs.at["solid biomass", "CO2 intensity"],
            efficiency2=costs.at["solid biomass", "CO2 intensity"]
            * costs.at["biomass CHP capture", "capture_rate"],
            lifetime=costs.at["solid biomass boiler steam CC", "lifetime"],
        )

        n.add("Bus", node+" coal", carrier = "coal")
        n.add(
            "Link",
            node+" coal for highT industry",
            bus0 = node+" coal",
            bus2="DE co2 atmosphere",
            bus1=node + " highT industry",
            carrier="coal for highT industry",
            p_nom_extendable=True,
            p_min_pu=must_run,
            efficiency=costs.at["direct firing solid fuels", "efficiency"],
            efficiency2=costs.at["coal", "CO2 intensity"],
            capital_cost=costs.at["direct firing solid fuels", "investment"]
            * costs.at["direct firing solid fuels", "efficiency"],
            marginal_cost=costs.at["direct firing solid fuels", "VOM"]
            + costs.at["biomass boiler", "pelletizing cost"],
            lifetime=costs.at["direct firing solid fuels", "lifetime"],
        )
        n.add(
            "Link",
            node+" coal for highT industry CC",
            bus0 = node+" coal",
            bus2="DE co2 stored",
            bus1=node + " highT industry",
            carrier="coal for highT industry CC",
            p_nom_extendable=True,
            p_min_pu=must_run,
            efficiency=costs.at["direct firing solid fuels", "efficiency"],
            efficiency2=costs.at["coal", "CO2 intensity"],
            capital_cost=costs.at["direct firing solid fuels", "investment"]
            * costs.at["direct firing solid fuels", "efficiency"]
            + costs.at["biomass CHP capture", "investment"]
            * costs.at["solid biomass", "CO2 intensity"],
            marginal_cost=costs.at["direct firing solid fuels", "VOM"]
            + costs.at["biomass boiler", "pelletizing cost"],
            lifetime=costs.at["direct firing solid fuels", "lifetime"],
        )
        n.add(
            "Link",
            node+" solid waste for highT industry",
            bus0 = "DE municipal solid waste",
            bus2="DE co2 atmosphere",
            bus1=node + " highT industry",
            carrier="waste for highT industry",
            p_nom_extendable=True,
            p_min_pu=must_run,
            efficiency=costs.at["direct firing solid fuels", "efficiency"],
            efficiency2=0.26, # 
            capital_cost=costs.at["direct firing solid fuels", "investment"]
            * costs.at["direct firing solid fuels", "efficiency"],
            marginal_cost=costs.at["direct firing solid fuels", "VOM"],
            lifetime=costs.at["direct firing solid fuels", "lifetime"],
        )

        n.add(
            "Link",
            node+" solid waste for highT industry CC",
            bus0 = "DE municipal solid waste",
            bus2="DE co2 stored",
            bus1=node + " highT industry",
            carrier="waste for highT industry",
            p_nom_extendable=True,
            p_min_pu=must_run,
            efficiency=costs.at["direct firing solid fuels", "efficiency"],
            efficiency2=0.26, # 
            capital_cost=costs.at["direct firing solid fuels", "investment"]
            * costs.at["direct firing solid fuels", "efficiency"]
            + costs.at["biomass CHP capture", "investment"]
            * costs.at["solid biomass", "CO2 intensity"],
            marginal_cost=costs.at["direct firing solid fuels", "VOM"],
            lifetime=costs.at["direct firing solid fuels", "lifetime"],
        )
        
        # add heat store for mediumT and highT 
        print("add heat store for lowT, mediumT and highT (150-1000 °C Sensible IRENA 2024, up to 1500°C by 2035 AGORA 2024)",
              "capital_cost = 100-25000 €/MWh (IRENA 2024), loss of 10 % per hour (AGORA 2024)", 
              "IRENA 2024: https://www.irena.org/Innovation-landscape-for-smart-electrification/Power-to-heat-and-cooling/7-Medium-and-high-temperature-thermal-energy-storage"
              )
        n.add("Store", "highT heat store", e_nom_extendable = True, 
              bus = node + " highT industry", carrier = "highT industry heat",
              standing_loss = 0.1, # 10 % efficiency, from AGORA 2024
              capital_cost = (100+25000)/2, # average https://www.irena.org/Innovation-landscape-for-smart-electrification/Power-to-heat-and-cooling/7-Medium-and-high-temperature-thermal-energy-storage
              e_cyclic = True)
        n.add("Store", "mediumT heat store", e_nom_extendable = True,
              bus = node + " mediumT industry", carrier = "mediumT industry heat",
              standing_loss = 0.1, # 10 % from AGORA 2024, 10-50% IRENA
              capital_cost = (100+25000)/2, # average https://www.irena.org/Innovation-landscape-for-smart-electrification/Power-to-heat-and-cooling/7-Medium-and-high-temperature-thermal-energy-storage
              e_cyclic = True)
        n.add("Store", "lowT heat store", e_nom_extendable = True,
                bus = node + " lowT industry", carrier = "lowT industry heat",
                standing_loss = 0.1, # 10 % from AGORA 2024, 10-50% IRENA
                capital_cost = (100+25000)/2, # average https://www.irena.org/Innovation-landscape-for-smart-electrification/Power-to-heat-and-cooling/7-Medium-and-high-temperature-thermal-energy-storage
                e_cyclic = True)
    return n 

def add_brownfield_industry(n):
    ''' 
    adds current industry plants (different data sources available)
    to the network as extendable links with p_nom_max and capital_cost 0
    '''
    ############################################################
    # Load and prepare NUTS3 data
    ############################################################
    file = "Plant_Data/NUTS_RG_03M_2016_4326_LEVL_3.shp/NUTS_RG_03M_2016_4326_LEVL_3.shp"
    nuts3 = gpd.read_file(file)

    nuts3['latitude'] = nuts3.geometry.centroid.y
    nuts3['longitude'] = nuts3.geometry.centroid.x
    nuts3 = nuts3.set_index('NUTS_ID')

    def haversine(powerplant_lat_lon_df, lat_column, lon_column, buses_lon, buses_lat):
        """Calculate the great circle distance between points."""
        # convert decimal degrees to radians
        lon_pp = np.radians(powerplant_lat_lon_df[lon_column])
        lat_pp = np.radians(powerplant_lat_lon_df[lat_column])
        lon_bus = np.radians(buses_lon)
        lat_bus = np.radians(buses_lat)
        
        # haversine formula 
        dlon = lon_bus - lon_pp  # x-distance
        dlat = lat_bus - lat_pp  # y-distance
        a = np.sin(dlat/2)**2 + np.cos(lat_pp) * np.cos(lat_bus) * np.sin(dlon/2)**2
        c = 2 * np.arcsin(np.sqrt(a)) 
        
        # Radius of earth in kilometers is 6371
        nearest_bus = (6371 * c).abs().sort_values().index[0]
        return nearest_bus, buses_lon[nearest_bus], buses_lat[nearest_bus]

    ############################################################
    # Functions to Process Plant Data
    ############################################################
    def get_aidres_plants_load(material):
        """Process AIDRES database output data."""
        # Load plant data from AIDRES database
        plants = pd.read_excel(
            "Plant_Data/AIDRES_D3.2-D_AIDRES database in Excel format_20230807.xlsx", 
            sheet_name="Product Flow",
            skiprows=12,
            index_col=0
        )

        # Filter for German NUTS3 regions
        plants = plants[
            (plants["Country code"]=="DE") & 
            (plants["NUTS LEVEL"]==3)
        ][[material, "NUTS_NAME"]]

        plants["Production_kt_y"] = (
            plants[material].fillna(0)
        )
        
        # Remove rows with zero or missing production
        plants = plants[
            plants[material].notna() & 
            (plants["Production_kt_y"] != 0)
        ]

        # Add latitude and longitude from NUTS3 regions
        plants["latitude"] = nuts3.loc[plants.index, "latitude"][nuts3.loc[plants.index, "CNTR_CODE"]=="DE"]
        plants["longitude"] = nuts3.loc[plants.index, "longitude"][nuts3.loc[plants.index, "CNTR_CODE"]=="DE"]

        # Calculate total output
        total_load = plants[material].sum()
        
        return plants, total_load # kt

    def get_aidres_plants_by_process_from_energy_carrier(sheet_name, material, material_energy_df, carrier, process):
        """
        AIDRES database give 2018 production volumes for Cement, Steel, Chemicals ("Product flow"), but not by process.
        The process (e.g. steamcracker) and/or specific product (HVC instead of all chemicals) can be derived from
        the energy carrier consumption at the site, with specific energy demands per .
        
        Parameters
        ----------
        sheet_name : str
            Name of the Excel sheet (e.g., "Naphtha (PJ per y)", "Coal (PJ per y)")
        material : str
            Name of the material column (e.g., "Chemical (PJ/y)", "Cement (PJ/y)", "Steel (PJ/y)")
        material_energy_df : pd.DataFrame
            DataFrame containing energy intensities for different processes
            (steel_energy, cement_energy or hvc_energy)
        carrier : str
            Name of the energy carrier in the steel_energy, cement_energy and hvc_energy dataframes 
            (e.g., "naphtha", "methane", "biomass")
        process : str
            Name of the industrial process in the steel_energy, cement_energy and hvc_energy dataframes 
            (e.g., "steamcracker", "CEM I")
        
            
        Returns
        -------
        pd.DataFrame
            Processed plant data with locations and production
        float
            Total production in kt/year
        """
        
        # Load plant data from AIDRES database
        plants = pd.read_excel(
            "Plant_Data/AIDRES_D3.2-D_AIDRES database in Excel format_20230807.xlsx", 
            sheet_name=sheet_name,
            skiprows=12,
            index_col=0
        )

        # Filter for German NUTS3 regions
        plants = plants[
            (plants["Country code"]=="DE") & 
            (plants["NUTS LEVEL"]==3)
        ][[material, "NUTS_NAME"]]

        # Calculate production in kt/year from energy consumption in PJ/year
        plants["Production_kt_y"] = (
            plants[material].fillna(0)*pj_to_mwh / 
            (material_energy_df.loc[carrier, process])
        )/1000
        
        # Remove rows with zero or missing production
        plants = plants[
            plants[material].notna() & 
            (plants["Production_kt_y"] != 0)
        ]

        # Add latitude and longitude from NUTS3 regions
        plants["latitude"] = nuts3.loc[plants.index, "latitude"][nuts3.loc[plants.index, "CNTR_CODE"]=="DE"]
        plants["longitude"] = nuts3.loc[plants.index, "longitude"][nuts3.loc[plants.index, "CNTR_CODE"]=="DE"]

        # Calculate total output
        total_load = plants[material].sum()*pj_to_mwh/(material_energy_df.loc[carrier, process])/1000
        
        return plants, total_load # kt

    ############################################################
    # Functions to Add Network Components
    ############################################################
    def assign_buses_to_plants(plants_df, lat_column, lon_column, network):
        """Assign nearest network bus to each plant."""
        # Create a copy to avoid modifying the original
        df = plants_df.copy()
        
        # Convert coordinates to float
        df[lon_column] = df[lon_column].astype(float)
        df[lat_column] = df[lat_column].astype(float)
        
        # Initialize bus columns
        df[["bus", "bus latitude", "bus longitude"]] = 0
        
        # Add bus column to df
        for i in range(len(df)):        
            nearest_bus = haversine(
                df.iloc[i], 
                lat_column,
                lon_column,
                network.buses.filter(like="DE", axis=0).x,
                network.buses.filter(like="DE", axis=0).y,
            )
            df.loc[df.index[i], ["bus", "bus latitude", "bus longitude"]] = nearest_bus
        
        df = df.fillna(0)
        return df

    ############################################################
    # Process Steel Plants
    ############################################################
    # Steel plant processing code
    # 2 possible data sources:
    # 1) IND-E database
    process_mapping = {
        'BF-BOF': 'ISW',
        'Scrap-EAF': 'EAF'
    }
    df = pd.read_excel('Plant_Data/steel_plants_df.xlsx', index_col=0)
    df['Process'] = df['Process'].replace(process_mapping)
    df_eaf = df[df['Process']=="EAF"] 
    df_isw = df[df['Process']=="ISW"] 
    production_column_eaf = "capacity (Mio. t steel/yr)"
    production_column_isw = "capacity (Mio. t steel/yr)"

    #2) AIDRES database
    aidres_steel, aidres_steel_production = get_aidres_plants_load(material = "Steel (kt/y)")

    aidres_steel_isw, aidres_isw_production = get_aidres_plants_by_process_from_energy_carrier(
        sheet_name="Coal (PJ per y)",
        material="Steel (PJ/y)",
        material_energy_df=steel_energy,
        carrier="coal",
        process="ISW"
    )

    aidres_steel["ISW_kt_y"] = 0
    aidres_steel.loc[aidres_steel_isw.index, "ISW_kt_y"] = aidres_steel_isw["Production_kt_y"]
    aidres_steel.loc[aidres_steel["ISW_kt_y"]<50, "ISW_kt_y"] = 0
    aidres_steel["EAF_kt_y"] = aidres_steel["Production_kt_y"] - aidres_steel["ISW_kt_y"]
    aidres_steel.loc["sum"] = aidres_steel.sum()
    df_eaf = aidres_steel[aidres_steel["EAF_kt_y"]>0]
    df_isw = aidres_steel[aidres_steel["ISW_kt_y"]>0]
    production_column_isw = "ISW_kt_y"
    production_column_eaf = "EAF_kt_y"

    # Assign buses to plants
    df_eaf_mapped = assign_buses_to_plants(
        plants_df=df_eaf,
        lat_column='latitude',
        lon_column='longitude', 
        network=n
    )

    df_isw_mapped = assign_buses_to_plants(
        plants_df=df_isw,
        lat_column='latitude',
        lon_column='longitude', 
        network=n
    )

    # Group plants by bus and process, summing their capacities
    df_isw_mapped = df_isw_mapped.groupby(['bus'])[production_column_isw].sum().reset_index()
    df_eaf_mapped = df_eaf_mapped.groupby(['bus'])[production_column_eaf].sum().reset_index()

    # add the industry sites to the network (now using grouped data)
    for _, row in df_isw_mapped.iterrows():
        p = "ISW"
        node = row["bus"]
        capacity = row[production_column_isw]
        
        n.add("Link",
            name = f"{node} steel {p}",
            carrier = p,
            bus0 = node,
            bus1 = "DE steel",
            p_nom_extendable= True,
            p_nom_max = capacity,
            p_min_pu=1,
            p_max_pu=1,
            efficiency=1/steel_energy.loc["elec", p],  # steel per elec
            bus2=f"{node} process emissions",
            efficiency2=steel_energy.loc["process emission", p]/steel_energy.loc["elec", p],
            bus3="DE gas for industry",
            efficiency3=-steel_energy.loc["methane", p]/steel_energy.loc["elec", p],
            bus4=f"{node} H2",
            efficiency4=-steel_energy.loc["hydrogen", p]/steel_energy.loc["elec", p],
            bus5="DE steel scrap",
            efficiency5=-steel_feedstock.loc["steel scrap", p]/steel_energy.loc["elec", p],
            bus6=f"{node} highT industry",
            efficiency6=-steel_energy.loc["furnaces heat", p]/steel_energy.loc["elec", p],
            marginal_cost=steel_energy.loc["mc_per_t", p]/steel_energy.loc["elec", p],
            capital_cost=0)

    for _, row in df_eaf_mapped.iterrows():
        p = "EAF"
        node = row["bus"]
        capacity = row[production_column_eaf]
        
        n.add("Link",
            name = f"{node} steel {p}",
            carrier = p,
            bus0 = node,
            bus1 = "DE steel",
            p_nom_extendable= True,
            p_nom_max = capacity,
            p_min_pu=1,
            p_max_pu=1,
            efficiency=1/steel_energy.loc["elec", p],  # steel per elec
            bus2=f"{node} process emissions",
            efficiency2=steel_energy.loc["process emission", p]/steel_energy.loc["elec", p],
            bus3="DE gas for industry",
            efficiency3=-steel_energy.loc["methane", p]/steel_energy.loc["elec", p],
            bus4=f"{node} H2",
            efficiency4=-steel_energy.loc["hydrogen", p]/steel_energy.loc["elec", p],
            bus5="DE steel scrap",
            efficiency5=-steel_feedstock.loc["steel scrap", p]/steel_energy.loc["elec", p],
            bus6=f"{node} highT industry",
            efficiency6=-steel_energy.loc["furnaces heat", p]/steel_energy.loc["elec", p],
            marginal_cost=steel_energy.loc["mc_per_t", p]/steel_energy.loc["elec", p],
            capital_cost=0)

    ############################################################
    # Process Cement Plants
    ############################################################
    # 2 data sources:
    # 1) Hotmaps database
    def cement_industrial_database():
        """Process cement plant data."""
        try:
            # Read cement plant data
            df_cement = pd.read_excel('Plant_Data/Industrial_database.xlsx', 
                            sheet_name='cement', 
                            index_col=0)
            
            # Extract coordinates from geom column
            def extract_coordinates(geom_str):
                if pd.isna(geom_str):
                    return None, None
                try:
                    # Extract values between POINT( and )
                    coords = geom_str.split('POINT(')[1].split(')')[0]
                    # Split into longitude and latitude
                    lon, lat = map(float, coords.split())
                    return lat, lon
                except:
                    return None, None
            
            # Apply coordinate extraction to each row
            df_cement[['Latitude', 'Longitude']] = pd.DataFrame(
                df_cement['geom'].apply(extract_coordinates).tolist(),
                index=df_cement.index
            )
            
            # Clean up the dataframe - keep only rows with production data
            df_cement = df_cement.dropna(subset=['geom'])
            # Calculate total cement output from Production 2021
            cement_load = df_cement['Production 2021 (kt, sum from IDEES)'].sum()
            return cement_load, df_cement
            
        except FileNotFoundError:
            print("Error: Industrial_database.xlsx not found in Plant_Data directory")
            return 0, pd.DataFrame()
        except Exception as e:
            print(f"Error processing cement plant data: {e}")
            return 0, pd.DataFrame()
    cement_load, df_cement = cement_industrial_database()

    # Process cement dataframe
    df_cement['longitude'] = df_cement['Longitude'].astype(float)
    df_cement['latitude'] = df_cement['Latitude'].astype(float)
    df_cement = df_cement[['CompanyName', 'SiteName', 'City', 'latitude', 'longitude', 'Production 2021 (kt, sum from IDEES)']]
    production_column = "Production 2021 (kt, sum from IDEES)"
    # 2) AIDRES database
    aidres_cement, aidres_cement_production = get_aidres_plants_load(material = "Cement (kt/y)")
    df_cement = aidres_cement
    production_column = "Production_kt_y"

    # Extract and map to nearest bus (using existing bus_coordinates and haversine function)
    #df_cement[["bus", "bus latitude", "bus longitude"]] = 0

    # Add bus column to df_cement
    df_cement = assign_buses_to_plants(df_cement, 'latitude', 'longitude', n)

    # Group plants by bus, summing their capacities
    grouped_df_cement = df_cement.groupby('bus')[production_column].sum().reset_index()

    # Add the cement sites to the network
    p = "CEM I"
    # add cement processes from Sarah's data, and the process heat fuel
    # for now, assume all are CEM I
    for _, row in grouped_df_cement.iterrows():
        node = row["bus"]
        capacity = row[production_column]
        
        n.add("Link",
            name=f"{node} cement {p}",
            carrier=f"cement {p}",
            bus0=node,
            bus1="DE cement",
            p_nom_extendable=True,
            p_nom_max = capacity,
            p_min_pu=1,
            p_max_pu=1,
            efficiency=1/cement_energy.loc["elec", p],  # cement per electricity
            bus2=node+" highT industry",                # high-temperature heat input
            efficiency2=-cement_energy.loc[["furnaces heat", "process heat"], p].sum()/cement_energy.loc["elec", p],
            bus3=node+" process emissions",  # process emissions
            efficiency3=cement_energy.loc["process emission", p]/cement_energy.loc["elec", p],
            marginal_cost=cement_energy.loc["mc_per_t", p]/cement_energy.loc["elec", p],
            capital_cost=0)

    ############################################################
    # Process HVC Plants
    ############################################################

    aidres_chem, aidres_hvc_production = get_aidres_plants_by_process_from_energy_carrier(
        sheet_name="Naphtha (PJ per y)",
        material="Chemical (PJ/y)",
        material_energy_df=hvc_energy,
        carrier="naphtha",
        process="steamcracker"
    )
    df_hvc = aidres_chem
    production_column = "Production_kt_y"

    # Add bus column to df_hvc
    df_hvc = assign_buses_to_plants(df_hvc, 'latitude', 'longitude', n)

    # Group plants by bus, summing their capacities
    df_hvc = df_hvc.groupby('bus')[production_column].sum().reset_index()
    p = "steamcracker"
    
    for _, row in df_hvc.iterrows():
        node = row["bus"]
        capacity = row[production_column]
        n.add("Link", node+" hvc "+p,
            p_nom_extendable = True,
            p_min_pu = 1, 
            p_max_pu = 1, 
            p_nom_max = capacity,
            carrier = "hvc "+p,
            bus0 = node,
            bus1 = "DE hvc",
            efficiency = (1/hvc_energy.loc["elec", p]),  # bus1/bus0, steel per elec
            bus2 = "DE oil for industry", 
            efficiency2 = -hvc_energy.loc["naphtha", p]/hvc_energy.loc["elec", p],
            bus3 = "DE methanol for industry",  
            efficiency3 = (-hvc_energy.loc["methanol", p]/hvc_energy.loc["elec", p]),
            bus5 = "DE gas for industry", 
            efficiency5 = (- hvc_energy.loc["methane", p]/hvc_energy.loc["elec", p]),
            bus6 = "DE plastic waste",
            efficiency6 = -hvc_feedstock.loc["plastic waste", p]/hvc_energy.loc["elec", p],                           
            marginal_cost = hvc_energy.loc["mc_per_t", p]/hvc_energy.loc["elec", p],
            capital_cost = 0)  
    





