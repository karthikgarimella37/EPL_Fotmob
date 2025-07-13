import streamlit as st
import pandas as pd
from db_connection import postgres_connection
from mplsoccer import FontManager, PyPizza
from highlight_text import fig_text

from scipy.stats import percentileofscore

# --- Define Available Metrics ---
METRICS_MAP = {
    # Per 90 Metrics
    "Goals/90": "goalsper90",
    "Assists/90": "assistsper90",
    "xG/90": "expectedgoalsper90",
    "xA/90": "expectedassistsper90",
    "Shots/90": "shotsper90",
    "Chances\n Created/90": "chancescreatedper90",
    "Touches/90": "touchesper90",
    "Tackles \n Won/90": "tackleswonper90",

    # Total Metrics
    "Total \nGoals": "totalgoals",
    "Total \nAssists": "totalassists",
    "Total \nShots": "totalshots",
    "Shots on \nTarget": "totalshotsontarget",
    "Accurate \nPasses": "totalaccuratepasses",
    "Successful \n Dribbles": "totalsuccessfuldribbles",
    "Passes into \n Final Third": "totalpassesintofinalthird",
    "Touches in \n Opp. Box": "totaltouchesinoppbox",
    "Tackles Won": "totaltackleswon",
    "Interceptions": "totalinterceptions",
    "Clearances": "totalheadedclearances",
    "Recoveries": "totalrecoveries",
    "Aerial \nDuels Won": "totalaerialduelswon",
    "Ground \nDuels Won": "totalgroundduelswon",
    "Was Fouled": "totalwasfouled",
    "Fouls Committed": "totalfoulscommitted",
    
    # Advanced Metrics
    "Total xG": "totalexpectedgoals",
    "Total xA": "totalexpectedassists",
    "xG + xA": "totalxgandxa",
    "Fantasy Points": "totalfantasypoints"
}

# --- Font Manager (with updated URLs) ---
@st.cache_resource
def load_fonts():
    """Load and cache fonts to prevent repeated downloads."""
    try:
        fm_rubik = FontManager('https://raw.githubusercontent.com/google/fonts/main/ofl/rubikmonoone/RubikMonoOne-Regular.ttf')
        return fm_rubik 
    except Exception as e:
        st.error(f"Error loading fonts: {e}")
        st.info("This can sometimes be caused by network issues or caching problems. Please try refreshing the page.")
        st.stop()

@st.cache_data
def load_seasonal_data():
    """Loads seasonal analytics data from the database."""
    try:
        engine = postgres_connection()
        with engine.connect() as conn:
            query = "SELECT * FROM vw_player_seasonal_analytics;"
            df = pd.read_sql(query, conn)
            print(df.head())
        return df
    except Exception as e:
        st.error(f"Database error while loading seasonal data: {e}")
        return pd.DataFrame()

def calculate_percentiles(df, metric_col, player_value):
    """Calculates the percentile rank of a player's metric."""
    # Ensure we don't divide by zero
    if player_value == 0 and df[metric_col].sum() == 0:
        return 0
    return int(percentileofscore(df[metric_col], player_value))

def run():
    """Main function to run the Player Comparison Dashboard."""
    st.header("Player Comparison Dashboard", anchor=False)
    
    # Load fonts using the cached function
    fm_rubik = load_fonts()

    df = load_seasonal_data()
    if df.empty:
        st.warning("No data loaded. Check connection and the 'vw_player_seasonal_analytics' view.")
        st.stop()
        
    # --- Sidebar Filters ---
    st.sidebar.header("📊 Comparison Filters")
    
    # Season Filter
    seasons = sorted(df['seasonname'].dropna().unique(), reverse=True)
    selected_season = st.sidebar.selectbox("Select Season", seasons)
    
    # Filter data based on season and minutes played
    season_df = df[(df['seasonname'] == selected_season) & (df['totalminutesplayed'] > 500)].copy()
    if season_df.empty:
        st.warning(f"No players found with over 500 minutes played in the {selected_season} season.")
        st.stop()

    # --- Dynamic Metric Selection ---
    st.sidebar.header("Select Metrics")
    # Default to all per-90 metrics
    default_params = ['Goals/90', 'Assists/90', 'xG/90', 'xA/90', 'Shots/90', 'Chances\n Created/90', 'Tackles \n Won/90',
                      'Ground \nDuels Won', 'Aerial \nDuels Won', 'Fantasy Points', 'Passes into \n Final Third', 'Successful \n Dribbles']
    valid_defaults = [p for p in default_params if p in METRICS_MAP]
    
    selected_params = st.sidebar.multiselect(
        "Choose metrics for comparison",
        options=list(METRICS_MAP.keys()),
        default=valid_defaults
    )

    if not selected_params:
        st.warning("Please select at least one metric to compare.")
        st.stop()

    # Player Selection - with specific defaults
    players = sorted(season_df['playername'].unique())
    
    p1_default = "Mohamed Salah"
    p2_default = "Erling Haaland"

    # Find index of default players, fall back to 0 and 1 if not found
    p1_index = players.index(p1_default) if p1_default in players else 0
    p2_index = players.index(p2_default) if p2_default in players else 1

    # Ensure the indices are different if one player is not found
    if p1_index == p2_index and len(players) > 1:
        p2_index = 1 if p1_index == 0 else 0

    player1_name = st.sidebar.selectbox("Select Player 1", players, index=p1_index)
    player2_name = st.sidebar.selectbox("Select Player 2", players, index=p2_index)

    if player1_name == player2_name:
        st.warning("Please select two different players for comparison.")
        st.stop()
        
    player1_data = season_df[season_df['playername'] == player1_name].iloc[0]
    player2_data = season_df[season_df['playername'] == player2_name].iloc[0]

    # --- Use selected parameters for percentile calculation ---
    metric_cols = [METRICS_MAP[p] for p in selected_params]
    
    player1_values = [calculate_percentiles(season_df, col, player1_data[col]) for col in metric_cols]
    player2_values = [calculate_percentiles(season_df, col, player2_data[col]) for col in metric_cols]

    # --- Plotting ---
    baker = PyPizza(
        params=selected_params,  # Use selected params for labels
        background_color="#EBEBE9",
        straight_line_color="#222222",
        straight_line_lw=1,
        last_circle_lw=1,
        last_circle_color="#222222",
        other_circle_ls="-.",
        other_circle_lw=1
    )

    fig, ax = baker.make_pizza(
        player1_values,
        compare_values=player2_values,
        figsize=(15, 8),
        kwargs_slices=dict(facecolor="#1A78CF", edgecolor="#222222", zorder=2, linewidth=1),
        kwargs_compare=dict(facecolor="#FF9300", edgecolor="#222222", zorder=2, linewidth=1),
        kwargs_params=dict(color="#000000", fontsize=8, fontproperties=fm_rubik.prop, va="center"),
        kwargs_values=dict(
            color="#000000", fontsize=10, fontproperties=fm_rubik.prop, zorder=3,
            bbox=dict(edgecolor="#000000", facecolor="cornflowerblue", boxstyle="round,pad=0.2", lw=1)
        ),
        kwargs_compare_values=dict(
            color="#000000", fontsize=10, fontproperties=fm_rubik.prop, zorder=3,
            bbox=dict(edgecolor="#000000", facecolor="#FF9300", boxstyle="round,pad=0.2", lw=1)
        )
    )

    # --- Title and Subtitle (using fig_text for highlighting) ---
    title_with_highlights = f"<{player1_name}> vs <{player2_name}>"
    subtitle_text = f"Percentile Rank vs Premier League Players | Season {selected_season}"

    fig_text(
        s=title_with_highlights,
        x=0.5, y=0.97,
        fig=fig,
        ha="center",
        fontsize=15,
        fontproperties=fm_rubik.prop,
        color="#000000",
        highlight_textprops=[{"color": "#1A78CF"}, {"color": "#FF9300"}]
    )

    fig_text(
        s=subtitle_text,
        x=0.5, y=0.94,
        fig=fig,
        ha="center",
        fontsize=10,
        fontproperties=fm_rubik.prop,
        color="#000000"
    )

    st.pyplot(fig) 