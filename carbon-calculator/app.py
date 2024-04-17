import streamlit as st
from firebase_admin import firestore,credentials
import firebase_admin
import pandas as pd
import datetime
# Firestore client
# Function to initialize Firebase if it hasn't been initialized already
def initialize_firebase():
    if not firebase_admin._apps:
        cred = credentials.Certificate("serviceKey.json")
        firebase_admin.initialize_app(cred)
    else:
        # If already initialized, use the existing app
        firebase_admin.get_app()
# Call the initialize function
initialize_firebase()
db = firestore.client()


# Add data to Firestore
def add_data_to_firestore(doc_id, data):
    db.collection("YourCollectionName").document(doc_id).set(data)
    st.success("Data added to Firestore!")

def update_constant(constants_group, constant, new_value): 
    """
    After selecting a site, get the local constants for that site and be able to update them
    """
    db.collection("constants").document(constants_group).update({constant: new_value})

formulation_cache = {}
def get_formulation(formulation_name): 
    if formulation_name in formulation_cache:
        return formulation_cache[formulation_name]
    else: 
        formulation = db.collection("formulations").document(formulation_name).get().to_dict()
        formulation_cache[formulation_name] = formulation
        return formulation

customer_cache = {}
def get_customer_distance(collection_prefix, customer_id): 
    if customer_id in customer_cache:
        return customer_cache[customer_id]
    else:
        customer_info = db.collection(f"{collection_prefix}/customers").document(customer_id).get()
        customer_info = customer_info.to_dict()
        customer_cache[customer_id] = customer_info.get("distance")
        return customer_info.get("distance")

supplier_cache = {}
def get_supplier_distance(collection_prefix, supplier_id):
    if supplier_id in supplier_cache:
        return supplier_cache[supplier_id]
    else:
        supplier_info = db.collection(f"{collection_prefix}/suppliers").document(supplier_id).get()
        supplier_info = supplier_info.to_dict()
        supplier_cache[supplier_id] = supplier_info.get("distance")
        return supplier_info.get("distance")

# Returns a tuple of two dataframes: carbon_retired, carbon_released, and total_biochar_prod
def do_accounting(site, start_date, end_date):
    """
    carbon leakage: date, T CO2eq, type
    carbon retired: order #, date, T CO2eq

    1. Delivered orders: 
    - if isActivated = False, get customer distance + transport costs
    - calculate % carbon of prodQty of order using formulation
    2. Inputs: 
    - if type = biomass, get supplier distance + vehicle type + transport costs 
    3. Carbon costs: 
    - if type = electricity, get qty * site-constant
    4. Biochar-prod: 
    - get total Tons production during that time period (+ CO2eq)
    - get methane emissions
    """
    global_constants = db.collection("constants").document("global").get().to_dict()
    site_constants = db.collection("constants").document(site).get().to_dict()
    carbon_retired = pd.DataFrame(columns=["Order #", "Date", "T Carbon", "T CO2eq"])
    carbon_released = pd.DataFrame(columns=["Date", "T CO2eq", "Type"])
    collection_path_prefix = f"sites/{site}" if site != "mock-site" else "test/mock-site"
    delivered_orders = db.collection(f"{collection_path_prefix}/orders").where("status", "==", "Delivered").where("deliveredDate", ">=", start_date).where("deliveredDate", "<=", end_date).get()
    for order in delivered_orders:
        order = order.to_dict()
        # calculate % carbon of prodQty of order using formulation
        formulation_name = order.get("formulation")[0]["id"]
        percent_carbon = get_formulation(formulation_name).get("Biochar")
        prod_qty_biochar = order.get("productionQuantity") * percent_carbon # Liters of biochar produced to fulfill this order
        biochar_density = site_constants.get("biocharDensityKgPerL")
        biochar_carbon_content = site_constants.get("biocharCarbonContent")
        tons_biochar = (prod_qty_biochar * biochar_density) / 1000
        tons_carbon = tons_biochar * biochar_carbon_content
        tons_co2 = tons_carbon * (44/12)
        print("FLAG", prod_qty_biochar, biochar_density, biochar_carbon_content, tons_biochar, tons_carbon, tons_co2, formulation_name, percent_carbon)
        df_row = {"Order #": order.get("orderNumber"), "Date": order.get("deliveredDate"), "T Carbon": tons_carbon, "T CO2eq": tons_co2}
        carbon_retired = pd.concat([carbon_retired, pd.DataFrame(df_row, index=[0])], ignore_index=True)
        if not order.get("isActivated"): 
            # get customer distance + transport costs
            customer_distance = get_customer_distance(collection_path_prefix, order.get("customer")[0]["id"]) # km
            vehicle = order.get("vehicle")[0]["id"] if order.get("vehicle") else None
            transport_cost = global_constants.get("transportKgCO2PerKm").get(vehicle) # kgCO2 per km
            print(vehicle, transport_cost, customer_distance)
            if transport_cost is None or customer_distance is None:
                raise ValueError("Missing vehicle, constant, or customer distance")
            tons_co2_transport = (transport_cost * customer_distance) / 1000
            df_row = {"Date": order.get("deliveredDate"), "T CO2eq": tons_co2_transport, "Type": "Raw Biochar Transport"}
            carbon_released = pd.concat([carbon_released, pd.DataFrame(df_row, index=[0])], ignore_index=True)
    inputs = db.collection(f"{collection_path_prefix}/inputs").where("date", ">=", start_date).where("date", "<=", end_date).where("type", "==", "Biomass").get()
    for input in inputs:
        input = input.to_dict()
        supplier_distance = get_supplier_distance(collection_path_prefix, input.get("supplier"))
        vehicle = input.get("vehicle")[0]["id"] if input.get("vehicle") else None
        transport_cost = site_constants.get(f"transportKgCO2PerKm.{vehicle}")
        if not transport_cost or not supplier_distance:
            raise ValueError("Missing vehicle, constant, or supplier distance")
        tons_co2_transport = (transport_cost * supplier_distance) / 1000
        df_row = {"Date": input.get("date"), "T CO2eq": tons_co2_transport, "Type": "Biomass Transport"}
        carbon_released = pd.concat([carbon_released, pd.DataFrame(df_row, index=[0])], ignore_index=True)
    carbon_costs = db.collection(f"{collection_path_prefix}/carbon-costs").where("date", ">=", start_date).where("date", "<=", end_date).get()
    for cost in carbon_costs:
        cost = cost.to_dict()
        qty = cost.get("value")
        if cost.get("type") == "Electricity":
            gramsCO2PerKWh = site_constants.get("gramsCO2PerKWh")
            tons_co2 = (qty * gramsCO2PerKWh) / 1000000
        elif cost.get("type") == "Diesel":
            dieselKgCO2PerL = global_constants.get("dieselKgCO2PerL")
            tons_co2 = (qty * dieselKgCO2PerL) / 1000
        df_row = {"Date": cost.get("date"), "T CO2eq": tons_co2, "Type": f"{cost.get('type')}: {cost.get('notes')}"}
        carbon_released = pd.concat([carbon_released, pd.DataFrame(df_row, index=[0])], ignore_index=True)
    biochar_produced = db.collection(f"{collection_path_prefix}/biochar-prod").where("endDate", ">=", start_date).where("endDate", "<=", end_date).get()
    total_biochar_prod = 0
    for production_record in biochar_produced:
        production_record = production_record.to_dict()
        total_biochar_prod += production_record.get("quantityTons")
    
    # Convert from datetime to date only
    carbon_released["Date"] = carbon_released["Date"].dt.date
    carbon_retired["Date"] = carbon_retired["Date"].dt.date

    return carbon_retired, carbon_released, total_biochar_prod






sites = list(map(lambda doc: doc.id, db.collection("sites").list_documents()))
st.title('DEC Carbon Credits Tracker')
accounting_tab, constants_tab = st.tabs(["Accounting", "Constants"])

with accounting_tab: 
    with st.form("query_form"):
        query_site = st.selectbox('Site', ["mock-site", *sites])
        col1, col2 = st.columns(2)
        with col1: 
            start_date = st.date_input('Start Date')
        with col2: 
            end_date = st.date_input('End Date')
        # Every form must have a submit button.
        submitted_query = st.form_submit_button("Submit")

    if submitted_query:
        start_date = datetime.datetime.combine(start_date, datetime.time(0, 0))
        end_date = datetime.datetime.combine(end_date, datetime.time(23, 59, 59))
        carbon_retired, carbon_released, total_biochar_prod = do_accounting(query_site, start_date, end_date)
        gross_carbon_offset = carbon_retired["T CO2eq"].sum()
        net_carbon_offset = gross_carbon_offset - carbon_released["T CO2eq"].sum()
        st.subheader("Summary")
        col1, col2, col3 = st.columns(3)
        col1.metric("Tons Biochar Produced", f"{round(total_biochar_prod, 3)} T")
        col2.metric("Gross CO2 Removed", f"{round(gross_carbon_offset, 3)} T")
        col3.metric("Net CO2 Removed", f"{round(net_carbon_offset, 3)} T")
        st.write("Carbon Released")
        carbon_released
        st.write("Carbon Retired")
        carbon_retired
        st.write("Note: Underestimate of retired carbon this time period b/c doesn't account for BCF formulated but not delivered (orders complete or in progress) which technically can be counted. Not a true carbon balance because carbon emissions may have happened previously out of scope of this time period and nothing is amortized.")
with constants_tab: 
    with st.expander("Global"): 
        global_constants = db.collection("constants").document("global").get().to_dict()
        for constant, currentValue in global_constants.items(): 
            if (type(currentValue) == dict):
                continue
            with st.form(key=constant):
                new_value = st.number_input(label=f"{constant}: {currentValue}", value=currentValue, key=f"value_{constant}")
                submitted = st.form_submit_button("Update Value")
                if submitted:
                    update_constant("global", constant, new_value)
                    st.success(f"Updated {constant} with new value: {new_value}")
    with st.expander("Transport"): 
        st.write("Transportation costs are measured in kgCO2 per km")
        transport_constants = db.collection("constants").document("global").get().to_dict().get("transportKgCO2PerKm")
        for vehicle, currentValue in transport_constants.items(): 
            with st.form(key=vehicle):
                new_value = st.number_input(label=f"{vehicle}: {currentValue}", value=currentValue, key=f"value_{vehicle}")
                submitted = st.form_submit_button("Update Value")
                if submitted:
                    update_constant("global", f"transportKgCO2PerKm.{vehicle}", new_value)
                    st.success(f"Updated {vehicle} with new value: {new_value}")
        with st.form(key="new_transport"): 
                st.write("Add a new form of transport here.")
                new_constant = st.text_input(label="Vehicle")
                new_value = st.number_input(label=f"kgCO2 per km")
                submitted = st.form_submit_button("Add New Vehicle")
                if submitted: 
                    update_constant("global", f"transportKgCO2PerKm.{new_constant}", new_value)
                    st.success(f"Added new vehicle: {new_constant} with value: {new_value}")
                    
    for site in sites: 
        with st.expander(site.capitalize()): 
            constants = db.collection("constants").document(site).get().to_dict()
            if constants:
                for constant, currentValue in constants.items(): 
                    with st.form(key=constant):
                        new_value = st.number_input(label=f"{constant}: {currentValue}", value=currentValue, key=f"value_{constant}")
                        submitted = st.form_submit_button("Update Value")
                        if submitted:
                            update_constant(site, constant, new_value)
                            st.success(f"Updated {constant} with new value: {new_value}")