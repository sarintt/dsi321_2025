import streamlit as st
import os
import pandas as pd

# Define the data fetching function
def data_from_lakefs(lakefs_endpoint: str = "http://localhost:8001/"):
    # Fetch the dataset from lakefs
    storage_options = {
        "key": os.getenv("ACCESS_KEY"),
        "secret": os.getenv("SECRET_KEY"),
        "client_kwargs": {
            "endpoint_url": lakefs_endpoint
        }
    }
    df = pd.read_parquet(
        "s3://tweets-repo/main/tweets.parquet",  # Replace with your actual path
        storage_options=storage_options,
        engine='pyarrow',
    )

    # Add year and month columns
    df["postTimeRaw"] = pd.to_datetime(df["postTimeRaw"])
    df["year"] = df["postTimeRaw"].dt.year.astype(int)
    df["month"] = df["postTimeRaw"].dt.strftime("%Y-%m")

    return df

# Dashboard Page
def show_dashboard():
    df = data_from_lakefs()
    st.title("📈 Tweet Tag Timeline Dashboard")
    
    # Select year range for the dashboard
    years = sorted(df["year"].unique())
    start_year, end_year = st.select_slider(
        "Select Year Range",
        options=years,
        value=(min(years), max(years))
    )
    
    df_filtered = df[(df["year"] >= start_year) & (df["year"] <= end_year)]
    
    # Count tags per month
    if 'tag' in df.columns:
        tag_counts = df_filtered.groupby("month")["tag"].count().reset_index(name="count")
    else:
        tag_counts = df_filtered.groupby("month").size().reset_index(name="count")
    
    tag_counts = tag_counts.sort_values("month")
    st.line_chart(tag_counts.set_index("month"))

# Dataset Page
def show_dataset():
    df = data_from_lakefs()
    st.title("🧾 Tweet Dataset")
    st.dataframe(df)

# Main app setup
st.set_page_config(page_title="Tweet Dashboard", layout="wide")

# Add a sidebar navigation menu
st.sidebar.title("Navigation")
page = st.sidebar.radio(
    "Go to",
    options=["Dashboard", "Dataset"]
)

# Display content based on selected page
if page == "Dashboard":
    show_dashboard()
elif page == "Dataset":
    show_dataset()
