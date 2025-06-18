import pandas as pd
          
def annualise(invest, lifetime, interest = 0.07):
    annuity_factor = interest * (1 + interest)**lifetime / ((1 + interest)**lifetime - 1)
    return round(invest * annuity_factor)

'''
def get_prices(): #this function will be replaced later (todo)
    prices = pd.read_excel("../data/raw/data_industry.xlsx", sheet_name = 'prices', index_col=0)
    energy_carriers_in_model = ['elec', 'biomass', 'methane', 'hydrogen', 'heat', 'steam']
    prices.loc[energy_carriers_in_model, "price 2020"] = 0
    prices_per_t = prices[prices["unit"] == "€/t"]["price 2020"]
    prices_per_MWh = prices[prices["unit"] == "€/MWh"]["price 2020"]
    return prices_per_MWh, prices_per_t
'''

# pypsa-eur industry data ========================================
def pypsa_industry_data():
    industry_sector_ratios = pd.read_csv("../data/industry_sector_ratios_DE_MWh_per_t.csv", index_col = 0)
    # if file not there, run ../data/DE_build_industry_sector_ratios.py
    outputs_today_pypsa = pd.read_csv("../data/pypsa-eur/industrial_production_per_country.csv", index_col = 0)
    outputs_tomorrow_pypsa = pd.read_csv("../data/pypsa-eur/industrial_production_per_country_tomorrow_2045.csv", index_col = 0)
    costs = pd.read_csv("../data/pypsa-eur/costs_2045.csv", index_col = [0,1], delimiter = ";")["value"]
    return industry_sector_ratios, outputs_today_pypsa, outputs_tomorrow_pypsa, costs
# industry model data =============================================
# steel
def steel_input(use_pypsa_data = True, years = range(2020, 2046)):
    
    industry_sector_ratios, outputs_today_pypsa, outputs_tomorrow_pypsa, costs = pypsa_industry_data()
    processes = ["Electric arc", "DRI + Electric arc", "Integrated steelworks"]  
    processes_today = ["Electric arc", "Integrated steelworks"]    
    steel_output = outputs_tomorrow_pypsa.loc["DE", processes].sum() 
    steel_output_today = outputs_today_pypsa.loc["DE", processes_today].sum()
    steel_energy = industry_sector_ratios[processes]
    
    steel_energy.loc["mc_per_t"] = 0
    steel_feedstock = pd.read_excel("../data/raw/data_industry.xlsx", sheet_name="steel feedstock", index_col = 0).iloc[0:8,:]
    prices = pd.read_excel("../data/raw/data_industry.xlsx", sheet_name = 'prices', index_col=0)
    prices_per_t = prices[prices["unit"] == "€/t"]["price 2020"]
    prices_per_MWh = prices[prices["unit"] == "€/MWh"]["price 2020"]
    # marginal prices
    for c in steel_feedstock.columns:
        mc = (steel_feedstock[c] * prices_per_t).sum()
        steel_energy.loc["mc_per_t", c] = mc
    # annualised capital prices
    steel_cost = pd.read_excel("../data/raw/data_industry.xlsx", 
        sheet_name="steel cost", index_col = 0).iloc[0:2,:]
    
    lifetime = 20
    for c in steel_energy.columns:
        ai = annualise(steel_cost.loc["2020 inv_cost [€/t output yearly]", c], lifetime)
        steel_energy.loc["an_inv_per_yt", c] = ai

    steel_feedstock.columns = ["EAF", "H2-DRI+EAF", "ISW"]  
    steel_feedstock["NG-DRI+EAF"] = steel_feedstock["H2-DRI+EAF"] 
    steel_energy.columns = ["EAF", "H2-DRI+EAF", "ISW"]  
    steel_energy["NG-DRI+EAF"] = steel_energy["H2-DRI+EAF"] 
    steel_energy.loc["methane", "NG-DRI+EAF"] = steel_energy.loc["hydrogen", "H2-DRI+EAF"]   
    steel_energy.loc["hydrogen", "NG-DRI+EAF"] = 0
    steel_energy.loc["process emission", "ISW"] += (
    steel_energy.loc["coal", "ISW"]
    *costs.loc["coal", "CO2 intensity"])  
       
    steel_prod_proj = pd.Series(index=years, data = [steel_output_today for y in years])
    #max_scrap, min_scrap = steel_limit_scrap(steel_prod_proj*1000, 2045) #steel_prod_proj in t
    df = calculate_secondary("steel", steel_prod_proj)
    max_scrap = df["waste [Mt]"].loc[2045]
    min_scrap = 0

    steel_todays_capacities = outputs_today_pypsa.loc["DE", processes_today]
    steel_todays_capacities.index = ["EAF", "ISW"]
    steel_todays_capacities["H2-DRI+EAF"] = 0
    steel_todays_capacities["NG-DRI+EAF"] = 0
    return steel_output, steel_output_today, steel_feedstock, steel_energy, steel_cost, steel_prod_proj, steel_todays_capacities, max_scrap, min_scrap  

# --- 
# hvc
def chem_input(use_pypsa_data = True, years = range(2020, 2046)):
    # option 1: output from model
    # hvc_todays_share, hvc_todays_cap, hvc_prod_proj = hvc_todays_capacities()
    
    # option 2: output from pypsa-eur
    if use_pypsa_data == True:
        industry_sector_ratios, outputs_today_pypsa, outputs_tomorrow_pypsa, costs = pypsa_industry_data()
        hvc_output = outputs_tomorrow_pypsa.loc["DE", ["HVC", "HVC (mechanical recycling)", "HVC (chemical recycling)"]].sum() 
        hvc_output_today = outputs_today_pypsa.loc["DE", "HVC"]
        hvc_prod_proj = pd.Series(index=years, data = [hvc_output_today for y in years])
    # kt
    hvc_output_today = 13089 # kt, from C4C p.25
    hvc_prod_proj = pd.Series(index=years, data = [hvc_output_today for y in years])
    print(f"hvc production projection: {hvc_output_today} kt assumed constant until 2045 (C4C p.25)")
    hvc_feedstock = pd.read_excel("../data/raw/data_industry.xlsx", sheet_name="hvc feedstock", index_col = 0).iloc[0:2,:]
    
    # processes not included in IDEES database because they are not yet used
    hvc_new_processes = pd.read_excel("../data/raw/data_industry.xlsx", sheet_name="hvc energy", index_col = 0).iloc[0:14,:]    
    hvc_cost = pd.read_excel("../data/raw/data_industry.xlsx", sheet_name="hvc cost", index_col = 0).iloc[0:2,:] 
    hvc_energy = industry_sector_ratios[["Basic chemicals | HVC", "Basic chemicals | HVC chemical recycling", "Basic chemicals | HVC mechanical recycling"]]
    hvc_energy.columns = ["steamcracker", "chemical recycling", "mechanical recycling"]
    hvc_energy["electric steamcracker"] = hvc_energy["steamcracker"]
    # electric steamcracker: same parameters as conventional but electricity instead of methane
    hvc_energy.loc["elec", "electric steamcracker"] += hvc_new_processes.loc["elec", "electric steamcracker"]
    hvc_energy.loc["methane", "electric steamcracker"]= 0
    hvc_energy.loc["methanol"] = 0
    hvc_energy["MtO"] = hvc_new_processes["MtO"]
    hvc_energy.loc["mc_per_t"] = 0 # cost of 0 assumed for plastic scrap
    hvc_energy.loc["an_inv_per_yt"] = 0
    lifetime = 20
    for c in hvc_energy.columns:
        print(c)
        ai = annualise(hvc_cost.loc["2020 inv_cost [€/t output yearly]", c], lifetime)
        hvc_energy.loc["an_inv_per_yt", c] = ai
    hvc_energy = hvc_energy.fillna(0)

    hvc_energy["Basic chemicals process heat"] = hvc_energy["steamcracker"] 
    hvc_energy.loc["elec": "process emission from feedstock", "Basic chemicals process heat"] = 0
    hvc_energy.loc["process heat":"furnaces heat","steamcracker"] = 0
    hvc_energy.loc["process heat":"furnaces heat","electric steamcracker"] = 0

    df = calculate_secondary("hvc", pd.Series(index = range(2020, 2046), data=hvc_output_today/1e3))
    max_waste = df["waste [Mt]"].loc[2045]
    min_waste = 0
    # Lightweight packaging waste today in Germany (for mechanical recycling): around 2.5 million t/year \cite{volk.2021}
    #kt
    max_packaging_waste = df["packaging waste [Mt]"].loc[2045]
    min_packaging_waste = 0

    hvc_feedstock = hvc_feedstock.fillna(0) # otherwise, nan are replaced with 1

    return hvc_output, hvc_output_today, hvc_feedstock, hvc_energy, hvc_cost, hvc_prod_proj, max_waste, min_waste, min_packaging_waste, max_packaging_waste 

# cement
def cement_input(use_pypsa_data = True, years = range(2020, 2046)):
  
    industry_sector_ratios, outputs_today_pypsa, outputs_tomorrow_pypsa, costs = pypsa_industry_data()
    # TODO add coal and waste kilns to highT options with mc coal and efficiency from pypsa
    # and coal EF 
    # review prices of supplementry materials / what materials can be chosen
    cement_feedstock = pd.read_excel("../data/raw/data_industry.xlsx", sheet_name="cement feedstock", index_col = 0).iloc[0:7,:]
    cement_cost = pd.read_excel("../data/raw/data_industry.xlsx", sheet_name="cement costs", index_col = 0).iloc[0:3,:]
    prices = pd.read_excel("../data/raw/data_industry.xlsx", sheet_name = 'prices', index_col=0)
    prices_per_t = prices[prices["unit"] == "€/t"]["price 2020"]

    # convert energy demand for cement to energy demand for clinker with current clinker factor 0.71
    industry_sector_ratios["Clinker"] =  industry_sector_ratios["Cement"]/0.71 # average clinker share 2020 is 0.71
    industry_sector_ratios.loc["production (kt)", "Clinker"] =  industry_sector_ratios.loc["production (kt)", "Cement"]*0.71 
    cement_energy = industry_sector_ratios[["Cement", "Clinker"]]
    cement_energy.loc["mc_per_t"] = 0
    cement_energy.loc["an_inv_per_yt"] = 0
    lifetime = 20

    # combine clinker factors with energy demand
    for c in cement_feedstock.columns:
        cement_energy[c] = cement_energy["Clinker"]*cement_feedstock.loc["clinker", c]
        cement_energy.loc["production (kt)", c] = cement_energy.loc["production (kt)", "Cement"]*cement_cost.loc["share 2022", c]
        ai = annualise(cement_cost.loc["2020 inv_cost [€/t output yearly]", c], lifetime)
        cement_energy.loc["an_inv_per_yt", c] = ai
        mc = (cement_feedstock.loc["SCM excl. BF slag and limestone":, c] * prices_per_t).sum()
        cement_energy.loc["mc_per_t", c] = mc
    
    cement_output = outputs_tomorrow_pypsa.loc["DE", "Cement"] 
    cement_output_today = outputs_today_pypsa.loc["DE", "Cement"]
    return cement_output, cement_output_today, cement_feedstock, cement_energy, cement_cost


def calculate_secondary(product, production):
    ''' 
    input:
    - product: "hvc" or "steel"
    - production: pandas series with production data for each year of 2020-2045
    (past rpoduction calculated within function)
    output:
    - dataframe with waste quantities for each year of years    
    '''
    products_net_export = {
    'hvc':   2,   # Mt 
    'steel': 0  # Mt
    }

    # packaging is treated separately
    end_sectors = {
        'transportation', 'mechanical engineering',
        'construction', 'other products', "electronics"
    }

    # Shares going to end-use sectors, from: Kullmann 2022 SI
    products_to_sectors = {
        "steel": {
            'transportation': 0.3,
            'mechanical engineering': 0.1,
            'construction': 0.47,
            'other products': 0.13,
            'electronics': 0
        },
        "hvc": {
            'transportation': 1509 / (881 + 3893 + 1509 + 3583 + 4369),
            'mechanical engineering': 0,
            'construction': 3583 / (881 + 3893 + 1509 + 3583 + 4369),  # pipes
            'other products': 3893 / (881 + 3893 + 1509 + 3583 + 4369),
            'packaging': 4369 / (881 + 3893 + 1509 + 3583 + 4369),
            'electronics': 881 / (881 + 3893 + 1509 + 3583 + 4369)
        }
    }

    # Lifetimes from: # p.17 https://plasticseurope.org/wp-content/uploads/2022/06/PlasticsEurope-CircularityReport-2022_2804-Light.pdf
    # Recovery rates for Germany: https://plasticseurope.org/wp-content/uploads/2023/02/PlasticsEurope-National_ALL.pdf
    sector_data = {
        'transportation': {
            'avg_lifetime': 13,  # Kullmann, plasticsEurope: 15
            'recovery_rate': 0.82
        },
        'mechanical engineering': {
            'avg_lifetime': 20,
            'recovery_rate': 0.87
        },
        'construction': {
            'avg_lifetime': 50,  # Kullmann and Plastics Europe
            'recovery_rate': 0.9 * 0.82  # 10% are obsolete = stay in construction sector, no recovery
        },
        'other products': {
            'avg_lifetime': 10,
            'recovery_rate': 0.58
        },
        'electronics': {
            'avg_lifetime': 5,  # plasticsEurope
            'recovery_rate': 0.2  # share of recycled electronics 2020, excl. thermal
        },
        'packaging': {
            'avg_lifetime': 1,
            'recovery_rate': 0.7 #corresponds to quality loss # Meys/Bardow 2020 table S-5
        }
        }
     
    ############################################################################################################
    # make dataframe for waste quantities for all years, 
    # starting with the maximum lifetime (50) before the first production year 
    ############################################################################################################
    years = range(2020, 2046)
    # df = pd.DataFrame(index = range(production_years[0],production_years[-1]+50+1))
    df = pd.DataFrame(index = range(years[0] - 50, years[-1]+1), columns = ['waste [Mt]'], data= 0)

    if product == "hvc": 
        #https://de.statista.com/statistik/daten/studie/167076/umfrage/produktionsmenge-der-deutschen-kunststoffindustrie-seit-2006/
        past_production = pd.Series(index = range(2006, 2023), 
                        data = [20.2,20.5,20,27.4,20.4, 20.2,19.5,19.9,18.2, 18.4,19.2,19.9,18.9,18.2, 18.2,21.3,14.3])
    
        ############################################################################################################
        # calculate waste 
        ############################################################################################################     

        for y in df.index:
            if y < past_production.index[0]:
                p = 0
            elif y <  years[0]:
                p = past_production.loc[y]
            else:
                p = production.loc[y]
            export = products_net_export[product]
            consumption = p - export
            for s in end_sectors: # xcludes packaging
                lifetime = sector_data[s]['avg_lifetime']
                share = products_to_sectors[product][s]
                recovery = sector_data[s]['recovery_rate']
                if y+lifetime < years[-1]+1:
                    df['waste [Mt]'].loc[y+lifetime] += (consumption*share*recovery)

        ############################################################################################################
        # in case of hvc: calculate packaging waste seperately
        ############################################################################################################    
        
        df['packaging waste [Mt]'] = 0
        s = "packaging"
        share = products_to_sectors[product][s]
        recovery = sector_data[s]['recovery_rate']
        # assumed to be recollected in the year of production
        df['packaging waste [Mt]'].loc[y] = consumption*share*recovery
        
        # the packaging waste not suitable for mechanical recycling (quality loss or sorted out) is added to mixed waste
        df['waste [Mt]'].loc[y] += df['packaging waste [Mt]'].loc[y]*(1-recovery) 
  
    elif product == "steel":
        # todo: add own calculation for steel, like hvc
        # read from plot: Kullmann 2022 SI (calculated with same method as hvc)
        for y in range(2020,2023):
            df.loc[y,'waste [Mt]'] = 30
        for y in range(2023,2035):
            df.loc[y,'waste [Mt]'] = ((31-30)/(2035-2023))*(y-2023)+30
        for y in range(2035,2046):
            df.loc[y,'waste [Mt]'] = 31.231218

    return df.loc[years]*1e3 # in kt
