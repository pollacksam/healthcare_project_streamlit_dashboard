import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np

st.set_page_config(page_title="Healthcare Dashboard", layout="wide")

# Read parquet file or partitioned parquet files within folder using Streamlit's native connection
@st.cache_data(ttl=600)
def load_parquets(s3_uri: str) -> pd.DataFrame:
    return pd.read_parquet(
        s3_uri,
        engine="pyarrow",
        storage_options={
            "key": st.secrets["AWS_ACCESS_KEY_ID"],
            "secret": st.secrets["AWS_SECRET_ACCESS_KEY"],
        },
    )

# S3 Path that you wish to connect
staffing_vs_occupancy = "s3://healthcare-project-sampollack/gold/staffing-vs-occupancy/"
overtime_hrs = "s3://healthcare-project-sampollack/gold/overtime-hrs/"
average_staffing = "s3://healthcare-project-sampollack/gold/average-staffing/"

# Load all three datasets
@st.cache_data(ttl=600)
def load_all_data():
    df_occupancy = load_parquets(staffing_vs_occupancy)
    df_overtime = load_parquets(overtime_hrs)
    df_staffing = load_parquets(average_staffing)

    return df_occupancy, df_overtime, df_staffing

try:
    df_occupancy, df_overtime, df_staffing = load_all_data()

except Exception as e:
    st.error(f"Unable to load S3 datasets: {e}")
    st.stop()

# Normalize CCN for consistent filtering
for df in [df_occupancy, df_overtime, df_staffing]:
    df["CCN_key"] = df["CCN"].astype("string").str.strip()

# Convert dates
df_staffing["WorkDate"] = pd.to_datetime(
    df_staffing["WorkDate"],
    errors="coerce"
)

# Ensure numeric columns are numeric
occupancy_numeric = [
    "occupancy_rate",
    "avg_residents_per_day",
    "avg_daily_hrs_rndon",
    "avg_daily_hrs_rnadmin",
    "avg_daily_hrs_rn",
    "avg_daily_hrs_lpnadmin",
    "avg_daily_hrs_lpn",
    "avg_daily_hrs_cna",
    "avg_daily_hrs_natrn",
    "avg_daily_hrs_medaide"
]

overtime_numeric = [
    "Total_OT_Hrs_RNDON",
    "Total_OT_Hrs_RNadmin",
    "Total_OT_Hrs_RN",
    "Total_OT_Hrs_LPNadmin",
    "Total_OT_Hrs_LPN",
    "Total_OT_Hrs_CNA",
    "Total_OT_Hrs_NAtrn",
    "Total_OT_Hrs_MedAide"
]

staffing_numeric = [
    "Hrs_RNDON",
    "Hrs_RNadmin",
    "Hrs_RN",
    "Hrs_LPNadmin",
    "Hrs_LPN",
    "Hrs_CNA",
    "Hrs_NAtrn",
    "Hrs_MedAide"
]

for col in occupancy_numeric:
    df_occupancy[col] = pd.to_numeric(
        df_occupancy[col], errors="coerce"
    )

for col in overtime_numeric:
    df_overtime[col] = pd.to_numeric(
        df_overtime[col], errors="coerce"
    )

for col in staffing_numeric:
    df_staffing[col] = pd.to_numeric(
        df_staffing[col], errors="coerce"
    )

# Shared Provider Filter
provider_data = pd.concat([
    df_occupancy[["CCN_key", "provider_name", "state"]],
    df_overtime[["CCN_key", "provider_name", "state"]],
    df_staffing[["CCN_key", "provider_name", "state"]]
], ignore_index=True)

provider_data = (
    provider_data
    .dropna(subset=["CCN_key"])
    .drop_duplicates(subset=["CCN_key"])
    .copy()
)

provider_data["provider_label"] = (
    provider_data["provider_name"].fillna("Unknown provider")
    + " (CCN: "
    + provider_data["CCN_key"].astype(str)
    + ")"
)

# Sidebar filters
st.sidebar.header("Filters")

states = sorted(
    provider_data["state"].dropna().unique().tolist()
)

selected_states = st.sidebar.multiselect(
    "Select State",
    options=states,
    default=[]
)

# Filter provider list based on selected states
filtered_providers = provider_data.copy()

if selected_states:

    filtered_providers = filtered_providers[
        filtered_providers["state"].isin(selected_states)
    ]

provider_options = filtered_providers[
    ["CCN_key", "provider_label"]
].drop_duplicates()

selected_providers = st.sidebar.multiselect(
    "Select Healthcare Provider",
    options=provider_options["provider_label"].tolist(),
    default=[]
)

# Convert selected provider labels into CCNs
selected_ccns = provider_options.loc[
    provider_options["provider_label"].isin(selected_providers),
    "CCN_key"
].tolist()

# Apply filters to each dataset independently
def apply_filters(df):
    result = df.copy()

    if selected_states:
        result = result[
            result["state"].isin(selected_states)
        ]

    if selected_providers:
        result = result[
            result["CCN_key"].isin(selected_ccns)
        ]

    return result

df_occupancy_filtered = apply_filters(df_occupancy)
df_overtime_filtered = apply_filters(df_overtime)
df_staffing_filtered = apply_filters(df_staffing)

# Dashboard Overview
st.header("Healthcare Staffing Overview")
st.subheader("April 2024 - June 2024")

col1, col2, col3 = st.columns(3)

col1.metric(
    "Healthcare Providers",
    f"{df_occupancy_filtered['CCN_key'].nunique():,}"
)

col2.metric(
    "Total Overtime Hours",
    f"{df_overtime_filtered[overtime_numeric].sum().sum():,.1f}"
)

col3.metric(
    "Average Occupancy Rate",
    f"{df_occupancy_filtered['occupancy_rate'].mean():.2f}"
    if df_occupancy_filtered["occupancy_rate"].notna().any()
    else "N/A"
)

st.divider()

# Dashboard tabs
tab1, tab2, tab3 = st.tabs([
    "Staffing vs Occupancy",
    "Overtime Analysis",
    "Average Staffing Trends"
])

# Tab 1: Staffing vs Occupancy
with tab1:

    st.subheader("Staffing vs Occupancy")

    st.caption(
        "Analyze provider occupancy and staffing hours "
        "across different nursing categories."
    )

    staffing_roles = {
        "Registered Nurses on duty": "avg_daily_hrs_rndon",
        "RN Administrators": "avg_daily_hrs_rnadmin",
        "Registered Nurses": "avg_daily_hrs_rn",
        "LPN Administrators": "avg_daily_hrs_lpnadmin",
        "Licensed Practical Nurses": "avg_daily_hrs_lpn",
        "Certified Nursing Assistants": "avg_daily_hrs_cna",
        "Nursing Assistants in Training": "avg_daily_hrs_natrn",
        "Medication Aides": "avg_daily_hrs_medaide"
    }

    selected_staffing_roles = st.multiselect(
        "Select Staffing Categories",
        options=list(staffing_roles.keys()),
        default=[
            "Registered Nurses",
            "Licensed Practical Nurses",
            "Certified Nursing Assistants"            
        ]
    )

    # Occupancy chart
    col1, col2 = st.columns(2)

    with col1:

        # Remove rows with missing occupancy rates
        occupancy_data = df_occupancy_filtered.dropna(
            subset=["occupancy_rate"]
        ).copy()

        if not occupancy_data.empty:

            # Create 20 equally spaced bins
            bin_edges = np.histogram_bin_edges(
                occupancy_data["occupancy_rate"],
                bins=20
            )

            occupancy_data["occupancy_bin"] = pd.cut(
                occupancy_data["occupancy_rate"],
                bins=bin_edges,
                include_lowest=True
            )

            # Create provider labels
            occupancy_data["provider_label"] = (
                occupancy_data["provider_name"].fillna("Unknown provider")
                + " (CCN: "
                + occupancy_data["CCN_key"].fillna("Unknown")
                + ")"
            )

            # Aggregate providers within each bin
            occupancy_bins = (
                occupancy_data
                .groupby("occupancy_bin", observed=True)
                .agg(
                    provider_count=("provider_label", "nunique"),
                    providers=(
                        "provider_label",
                        lambda x: "<br>".join(
                            sorted(x.dropna().unique())
                        )
                    )
                )
                .reset_index()
            )

            # Create readable bin labels
            occupancy_bins["bin_label"] = (
                occupancy_bins["occupancy_bin"].apply(
                    lambda x: f"{x.left:.3f} - {x.right:.3f}"
                )
            )

            # Create bar chart
            fig_occupancy = px.bar(
                occupancy_bins,
                x="bin_label",
                y="provider_count",
                custom_data=["providers"],
                title="Distribution of Occupancy Rates",
                labels={
                    "bin_label": "Occupancy Rate",
                    "provider_count": "Number of Providers"
                }
            )

            # Customize hover information
            fig_occupancy.update_traces(
                hovertemplate=
                    "Occupancy Rate: %{x}<br>"
                    "Number of Providers: %{y}<br>"
                    "Providers:<br>"
                    "%{customdata[0]}"
                    "<extra></extra>"
            )

            st.plotly_chart(
                fig_occupancy,
                width='stretch'
            )

        else:
            st.info("No occupancy data available for the selected filters.")

    with col2:

        # Remove rows with missing average resident values
        residents_data = df_occupancy_filtered.dropna(
            subset=["avg_residents_per_day"]
        ).copy()

        if not residents_data.empty:

            # Create 20 equally spaced bins
            resident_bin_edges = np.histogram_bin_edges(
                residents_data["avg_residents_per_day"],
                bins=20
            )

            residents_data["resident_bin"] = pd.cut(
                residents_data["avg_residents_per_day"],
                bins=resident_bin_edges,
                include_lowest=True
            )

            # Create provider labels
            residents_data["provider_label"] = (
                residents_data["provider_name"].fillna("Unknown provider")
                + " (CCN: "
                + residents_data["CCN_key"].fillna("Unknown")
                + ")"
            )

            # Aggregate providers within each bin
            resident_bins = (
                residents_data
                .groupby("resident_bin", observed=True)
                .agg(
                    provider_count=("provider_label", "nunique"),
                    providers=(
                        "provider_label",
                        lambda x: "<br>".join(
                            sorted(x.dropna().unique())
                        )
                    )
                )
                .reset_index()
            )

            # Create readable bin labels
            resident_bins["bin_label"] = (
                resident_bins["resident_bin"].apply(
                    lambda x: f"{x.left:.1f} - {x.right:.1f}"
                )
            )

            # Create bar chart
            fig_residents = px.bar(
                resident_bins,
                x="bin_label",
                y="provider_count",
                custom_data=["providers"],
                title="Average Daily Residents by Provider",
                labels={
                    "bin_label": "Average Residents Per Day",
                    "provider_count": "Number of Providers"
                }
            )

            # Customize hover information
            fig_residents.update_traces(
                hovertemplate=
                    "Average Daily Residents: %{x}<br>"
                    "Number of Providers: %{y}<br>"
                    "Providers:<br>"
                    "%{customdata[0]}"
                    "<extra></extra>"
            )

            st.plotly_chart(
                fig_residents,
                width='stretch'
            )

        else:
            st.info("No average daily residents data available for the selected filters.")

    # Staffing hours by role
    if selected_staffing_roles:
        staffing_plot = df_occupancy_filtered[
            ["provider_name"] +
            [staffing_roles[r] for r in selected_staffing_roles]
        ].melt(
            id_vars="provider_name",
            var_name="Staffing Category",
            value_name="Daily Hours"
        )

        role_labels = {
            value: key
            for key, value in staffing_roles.items()
        }

        staffing_plot["Staffing Category"] = (
            staffing_plot["Staffing Category"].map(role_labels)
        )

        fig_staffing = px.bar(
            staffing_plot,
            x="provider_name",
            y="Daily Hours",
            color="Staffing Category",
            barmode="group",
            title="Average Daily Staffing Hours by Provider"
        )

        st.plotly_chart(
            fig_staffing,
            width='stretch'
        )


    
    # Staffing levels relative to patient load
    st.divider()

    st.subheader(
        "Facilities with Lowest Staffing Relative to Patient Load"
    )

    st.caption(
        "Compares facility staffing hours per resident against "
        "the statewide average."
    )

    if selected_staffing_roles:

        # Get columns for selected staffing categories
        selected_staffing_columns = [
            staffing_roles[role]
            for role in selected_staffing_roles
        ]

        # -----------------------------------------
        # 1. Calculate facility-level staffing ratios
        # -----------------------------------------

        staffing_ratio_data = df_occupancy_filtered.copy()

        staffing_ratio_data["total_daily_staffing_hours"] = (
            staffing_ratio_data[selected_staffing_columns]
            .sum(axis=1, min_count=1)
        )

        staffing_ratio_data = staffing_ratio_data[
            staffing_ratio_data["avg_residents_per_day"].notna()
            & (staffing_ratio_data["avg_residents_per_day"] > 0)
        ].copy()

        staffing_ratio_data = staffing_ratio_data.dropna(
            subset=["total_daily_staffing_hours"]
        )

        staffing_ratio_data["staffing_hours_per_resident"] = (
            staffing_ratio_data["total_daily_staffing_hours"]
            / staffing_ratio_data["avg_residents_per_day"]
        )

        # -----------------------------------------
        # 2. Calculate statewide staffing benchmark
        # -----------------------------------------

        # Start with the FULL occupancy dataset.
        # This intentionally ignores the provider dropdown filter.

        benchmark_data = df_occupancy.copy()

        # Respect the selected state filter
        if selected_states:
            benchmark_data = benchmark_data[
                benchmark_data["state"].isin(selected_states)
            ].copy()

        # Calculate combined daily staffing hours
        benchmark_data["total_daily_staffing_hours"] = (
            benchmark_data[selected_staffing_columns]
            .sum(axis=1, min_count=1)
        )

        # Exclude missing or invalid resident counts
        benchmark_data = benchmark_data[
            benchmark_data["avg_residents_per_day"].notna()
            & (benchmark_data["avg_residents_per_day"] > 0)
        ].copy()

        benchmark_data = benchmark_data.dropna(
            subset=["total_daily_staffing_hours"]
        )

        # Calculate weighted statewide average
        total_state_staffing_hours = (
            benchmark_data["total_daily_staffing_hours"].sum()
        )

        total_state_residents = (
            benchmark_data["avg_residents_per_day"].sum()
        )

        if total_state_residents > 0:

            statewide_average = (
                total_state_staffing_hours
                / total_state_residents
            )

            # Determine benchmark label
            if len(selected_states) == 1:
                benchmark_label = (
                    f"{selected_states[0]} Average"
                )
            elif selected_states:
                benchmark_label = "Selected States Average"
            else:
                benchmark_label = "Overall Dataset Average"

            # -----------------------------------------
            # 3. Display statewide benchmark
            # -----------------------------------------

            st.metric(
                label=f"{benchmark_label} Staffing Hours per Resident",
                value=f"{statewide_average:.2f}"
            )

            # -----------------------------------------
            # 4. Create bottom 10 facility ranking
            # -----------------------------------------

            if not staffing_ratio_data.empty:

                staffing_ratio_data["provider_label"] = (
                    staffing_ratio_data["provider_name"].fillna(
                        "Unknown provider"
                    )
                    + " (CCN: "
                    + staffing_ratio_data["CCN_key"].fillna("Unknown")
                    + ")"
                )

                bottom_10_staffing = (
                    staffing_ratio_data
                    .nsmallest(
                        10,
                        "staffing_hours_per_resident"
                    )
                    .sort_values(
                        "staffing_hours_per_resident",
                        ascending=False
                    )
                )

                # -----------------------------------------
                # 5. Create horizontal bar chart
                # -----------------------------------------

                fig_staffing_ratio = px.bar(
                    bottom_10_staffing,
                    x="staffing_hours_per_resident",
                    y="provider_label",
                    orientation="h",
                    custom_data=[
                        "CCN_key",
                        "total_daily_staffing_hours",
                        "avg_residents_per_day"
                    ],
                    title=(
                        "Bottom 10 Facilities by Staffing Hours "
                        "per Resident"
                    ),
                    labels={
                        "staffing_hours_per_resident":
                            "Staffing Hours per Resident per Day",
                        "provider_label": "Healthcare Provider"
                    }
                )

                # Add statewide average reference line
                fig_staffing_ratio.add_vline(
                    x=statewide_average,
                    line_dash="dash",
                    line_color="#E5A13A",
                    line_width=2,
                    annotation_text=(
                        f"{benchmark_label}: "
                        f"{statewide_average:.2f}"
                    ),
                    annotation_position="top"
                )

                # Customize hover information
                fig_staffing_ratio.update_traces(
                    hovertemplate=
                        "Provider: %{y}<br>"
                        "CCN: %{customdata[0]}<br>"
                        "Staffing Hours per Resident: %{x:.2f}<br>"
                        "Total Daily Staffing Hours: "
                        "%{customdata[1]:.1f}<br>"
                        "Average Daily Residents: "
                        "%{customdata[2]:.1f}"
                        "<extra></extra>"
                )

                fig_staffing_ratio.update_layout(
                    xaxis_title=(
                        "Staffing Hours per Resident per Day"
                    ),
                    yaxis_title="Healthcare Provider",
                    showlegend=False
                )

                st.plotly_chart(
                    fig_staffing_ratio,
                    width='stretch'
                )

            else:
                st.info(
                    "No usable facility staffing data available "
                    "for the selected filters."
                )

        else:
            st.info(
                "Unable to calculate the statewide staffing "
                "benchmark for the selected categories and states."
            )

    else:
        st.info(
            "Please select at least one staffing category "
            "to calculate staffing hours per resident."
        )
    st.subheader("Staffing vs Occupancy Data")

    st.dataframe(
        df_occupancy_filtered.drop(columns=["CCN_key"]),
        width='stretch'
    )


# Tab 2: Overtime Analysis
with tab2:

    st.subheader("Overtime Hours Analysis")

    overtime_roles = {
        "Registered Nurses on duty": "Total_OT_Hrs_RNDON",
        "RN Administrators": "Total_OT_Hrs_RNadmin",
        "Registered Nurses": "Total_OT_Hrs_RN",
        "LPN Administrators": "Total_OT_Hrs_LPNadmin",
        "Licensed Practical Nurses": "Total_OT_Hrs_LPN",
        "Certified Nursing Assistants": "Total_OT_Hrs_CNA",
        "Nursing Assistants in Training": "Total_OT_Hrs_NAtrn",
        "Medication Aides": "Total_OT_Hrs_MedAide"
    }

    # --------------------------------------------------
    # 1. Prepare facility-level overtime data
    # --------------------------------------------------

    # Aggregate to one row per facility (if necessary).
    overtime_facility = (
        df_overtime_filtered
        .groupby(
            ["CCN_key", "provider_name", "state"],
            as_index=False,
            dropna=False
        )[overtime_numeric]
        .sum(min_count=1)
    )

    total_facilities = overtime_facility["CCN_key"].nunique()

    if total_facilities == 0:
        st.warning("No overtime data is available for the selected filters.")

    else:

        # --------------------------------------------------
        # 2. Overall overtime summary
        # --------------------------------------------------

        total_overtime = (
            overtime_facility[overtime_numeric]
            .sum()
            .sum()
        )

        facilities_with_any_overtime = (
            overtime_facility[overtime_numeric]
            .fillna(0)
            .gt(0)
            .any(axis=1)
            .sum()
        )

        overall_facility_percentage = (
            facilities_with_any_overtime / total_facilities * 100
        )

        col1, col2, col3 = st.columns(3)

        col1.metric(
            "Total Overtime Hours",
            f"{total_overtime:,.1f}"
        )

        col2.metric(
            "Facilities Reporting Overtime",
            f"{facilities_with_any_overtime:,} / {total_facilities:,}"
        )

        col3.metric(
            "Facilities with Overtime (%)",
            f"{overall_facility_percentage:.1f}%"
        )

        st.caption(
            "Facility percentages measure the share of facilities "
            "with reported overtime hours greater than zero. They do "
            "not represent the percentage of individual nurses "
            "working overtime."
        )

        st.divider()

        # --------------------------------------------------
        # 3. Overtime by staffing category
        # --------------------------------------------------

        overtime_by_role = (
            overtime_facility[overtime_roles.values()]
            .sum()
            .rename_axis("Overtime Column")
            .reset_index(name="Overtime Hours")
        )

        role_labels = {
            value: key
            for key, value in overtime_roles.items()
        }

        overtime_by_role["Staffing Category"] = (
            overtime_by_role["Overtime Column"].map(role_labels)
        )

        # Count facilities with overtime in each category.
        facility_counts_by_role = []

        for role, column in overtime_roles.items():

            facilities_with_role_overtime = (
                overtime_facility[column].fillna(0).gt(0).sum()
            )

            facility_counts_by_role.append({
                "Staffing Category": role,
                "Facilities with Overtime": facilities_with_role_overtime,
                "Percentage of Facilities": (
                    facilities_with_role_overtime / total_facilities * 100
                )
            })

        facility_counts_by_role = pd.DataFrame(
            facility_counts_by_role
        )

        overtime_by_role = overtime_by_role.merge(
            facility_counts_by_role,
            on="Staffing Category",
            how="left"
        )

        st.subheader("Overtime by Staffing Category")

        col1, col2 = st.columns(2)

        with col1:

            fig_ot_role = px.bar(
                overtime_by_role.sort_values(
                    "Overtime Hours",
                    ascending=False
                ),
                x="Staffing Category",
                y="Overtime Hours",
                title="Total Overtime Hours by Category",
                color="Overtime Hours"
            )

            fig_ot_role.update_layout(
                xaxis_title="Staffing Category",
                yaxis_title="Total Overtime Hours",
                xaxis_tickangle=-45
            )

            st.plotly_chart(
                fig_ot_role,
                width='stretch'
            )

        with col2:

            fig_facility_percentage = px.bar(
                overtime_by_role.sort_values(
                    "Percentage of Facilities",
                    ascending=False
                ),
                x="Staffing Category",
                y="Percentage of Facilities",
                title="% of Facilities Reporting Overtime",
                color="Percentage of Facilities",
                range_y=[0, 100]
            )

            fig_facility_percentage.update_layout(
                xaxis_title="Staffing Category",
                yaxis_title="% of Facilities",
                xaxis_tickangle=-45
            )

            st.plotly_chart(
                fig_facility_percentage,
                width='stretch'
            )

        st.dataframe(
            overtime_by_role[
                [
                    "Staffing Category",
                    "Overtime Hours",
                    "Facilities with Overtime",
                    "Percentage of Facilities"
                ]
            ].style.format({
                "Overtime Hours": "{:,.1f}",
                "Percentage of Facilities": "{:.1f}%"
            }),
            width='stretch',
            hide_index=True
        )

        st.divider()

        # --------------------------------------------------
        # 4. Detailed analysis by selected staffing category
        # --------------------------------------------------

        st.subheader("Staffing Category Deep Dive")

        selected_overtime_role = st.selectbox(
            "Select Staffing Category",
            options=list(overtime_roles.keys()),
            index=2,
            key="overtime_category"
        )

        selected_overtime_column = overtime_roles[
            selected_overtime_role
        ]

        # Create a facility-level dataset for the selected category.
        selected_role_data = overtime_facility[
            [
                "CCN_key",
                "provider_name",
                "state",
                selected_overtime_column
            ]
        ].copy()

        selected_role_data = selected_role_data.rename(
            columns={
                selected_overtime_column: "Overtime Hours"
            }
        )

        # Exclude missing overtime values from the calculation.
        # Missing values are not automatically treated as zero.
        reported_role_data = selected_role_data.dropna(
            subset=["Overtime Hours"]
        ).copy()

        facilities_reporting_data = len(reported_role_data)

        facilities_with_role_overtime = (
            reported_role_data["Overtime Hours"] > 0
        ).sum()

        role_facility_percentage = (
            facilities_with_role_overtime / total_facilities * 100
        )

        total_role_overtime = reported_role_data[
            "Overtime Hours"
        ].sum()

        average_role_overtime = (
            reported_role_data["Overtime Hours"].mean()
            if not reported_role_data.empty
            else 0
        )

        median_role_overtime = (
            reported_role_data["Overtime Hours"].median()
            if not reported_role_data.empty
            else 0
        )

        # Display selected category metrics.
        col1, col2, col3, col4 = st.columns(4)

        col1.metric(
            "Total Overtime Hours",
            f"{total_role_overtime:,.1f}"
        )

        col2.metric(
            "Facilities with Overtime",
            f"{facilities_with_role_overtime:,}"
        )

        col3.metric(
            "Facilities with Overtime (%)",
            f"{role_facility_percentage:.1f}%"
        )

        col4.metric(
            "Average Hours per Reporting Facility",
            f"{average_role_overtime:,.1f}"
        )

        st.metric(
            "Median Overtime Hours per Reporting Facility",
            f"{median_role_overtime:,.1f}"
        )

        st.caption(
            f"{facilities_reporting_data:,} facilities have a reported "
            f"overtime value for {selected_overtime_role}. The percentage "
            f"above uses all {total_facilities:,} facilities in the "
            "current filtered dataset as its denominator."
        )

        # --------------------------------------------------
        # 5. Top facilities for selected category
        # --------------------------------------------------

        st.subheader(
            f"Top 15 Facilities by {selected_overtime_role} Overtime"
        )

        top_role_facilities = (
            reported_role_data
            .sort_values(
                "Overtime Hours",
                ascending=False
            )
            .head(15)
        )

        fig_top_role = px.bar(
            top_role_facilities,
            x="provider_name",
            y="Overtime Hours",
            hover_data=["state", "CCN_key"],
            title=f"Top 15 Facilities - {selected_overtime_role}",
            color="Overtime Hours"
        )

        fig_top_role.update_layout(
            xaxis_title="Healthcare Provider",
            yaxis_title="Overtime Hours",
            xaxis_tickangle=-45
        )

        st.plotly_chart(
            fig_top_role,
            width='stretch'
        )

        # --------------------------------------------------
        # 6. Facility-level data for selected category
        # --------------------------------------------------

        st.subheader(
            f"{selected_overtime_role} Facility Data"
        )

        st.dataframe(
            reported_role_data.sort_values(
                "Overtime Hours",
                ascending=False
            ).drop(columns=["CCN_key"]),
            width='stretch',
            hide_index=True
        )


# Tab 3: Average Staffing
with tab3:

    st.subheader("Average Staffing Trends")

    average_roles = {
        "Registered Nurses on duty": "Hrs_RNDON",
        "RN Administrators": "Hrs_RNadmin",
        "Registered Nurses": "Hrs_RN",
        "LPN Administrators": "Hrs_LPNadmin",
        "Licensed Practical Nurses": "Hrs_LPN",
        "Certified Nursing Assistants": "Hrs_CNA",
        "Nursing Assistants in Training": "Hrs_NAtrn",
        "Medication Aides": "Hrs_MedAide"
    }

    selected_average_roles = st.multiselect(
        "Select Staffing Categories",
        options=list(average_roles.keys()),
        default=[
            "Registered Nurses",
            "Licensed Practical Nurses",
            "Certified Nursing Assistants"
        ],
        key="average_staffing_roles"
    )

    if selected_average_roles:

        selected_columns = [
            average_roles[r]
            for r in selected_average_roles
        ]

        # Aggregate daily hours across selected providers.
        # This represents total daily hours, not provider averages.

        daily_staffing = (
            df_staffing_filtered
            .groupby("WorkDate")[selected_columns]
            .sum(min_count=1)
            .reset_index()
        )

        daily_staffing_plot = daily_staffing.melt(
            id_vars="WorkDate",
            var_name="Staffing Category",
            value_name="Staffing Hours"
        )

        role_labels = {
            value: key
            for key, value in average_roles.items()
        }

        daily_staffing_plot["Staffing Category"] = (
            daily_staffing_plot["Staffing Category"].map(role_labels)
        )

        fig_daily_staffing = px.line(
            daily_staffing_plot,
            x="WorkDate",
            y="Staffing Hours",
            color="Staffing Category",
            title="Daily Staffing Hours by Category",
            markers=True
        )

        st.plotly_chart(
            fig_daily_staffing,
            width='stretch'
        )

    st.subheader("Average Staffing Data")

    st.dataframe(
        df_staffing_filtered.drop(columns=["CCN_key"]),
        width='stretch'
    )