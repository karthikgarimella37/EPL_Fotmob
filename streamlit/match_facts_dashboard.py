import streamlit as st
import pandas as pd
import json
import matplotlib.pyplot as plt
from db_connection import postgres_connection

@st.cache_data
def load_match_facts_data():
    """Loads match facts data from the database."""
    try:
        engine = postgres_connection()
        with engine.connect() as conn:
            query = "SELECT * FROM vw_match_facts;"
            df = pd.read_sql(query, conn)
        # Convert momentum from string to dict
        df['momentum'] = df['momentum'].apply(lambda x: json.loads(x) if x else None)
        return df
    except Exception as e:
        st.error(f"Database error loading match facts: {e}")
        return pd.DataFrame()

def create_momentum_chart(match_data):
    """Creates and returns a matplotlib figure for the momentum chart."""
    try:
        momentum_data = match_data['momentum'].iloc[0]
        # Check for empty or malformed data
        if not momentum_data or 'main' not in momentum_data or 'data' not in momentum_data['main'] or not momentum_data['main']['data']:
            return None

        # Create the DataFrame from the correct path
        df_momentum = pd.DataFrame(momentum_data['main']['data'])
        
        if df_momentum.empty or 'value' not in df_momentum.columns:
            return None

        df_momentum['value'] = pd.to_numeric(df_momentum['value'])
        
        home_color = match_data['hometeamcolor'].iloc[0] if match_data['hometeamcolor'].iloc[0] else '#d3151e'
        away_color = match_data['awayteamcolor'].iloc[0] if match_data['awayteamcolor'].iloc[0] else '#4a72d4'
        
        fig, ax = plt.subplots(figsize=(8, 4))
        fig.set_facecolor('#121212')
        ax.set_facecolor('#121212')

        ax.fill_between(df_momentum.index, 0, df_momentum['value'], where=df_momentum['value'] >= 0, interpolate=True, color=home_color, alpha=0.8)
        ax.fill_between(df_momentum.index, 0, df_momentum['value'], where=df_momentum['value'] < 0, interpolate=True, color=away_color, alpha=0.8)
        
        ax.axhline(0, color='white', linewidth=0.5)
        ax.set_xticks([0, len(df_momentum) / 2, len(df_momentum)])
        ax.set_xticklabels(["0'", 'HT', 'FT'], color='white', fontsize=12)
        ax.set_yticks([])
        
        for spine in ax.spines.values():
            spine.set_visible(False)
            
        ax.set_title('Momentum', color='white', fontsize=16)
        return fig
    except Exception:
        # If any error occurs, just skip creating the chart
        return None

def get_contrasting_text_color(hex_color):
    """Returns black or white for text depending on the background hex color's brightness."""
    if not hex_color:
        return '#FFFFFF'
    hex_color = hex_color.lstrip('#')
    r, g, b = tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))
    # Calculate luminance
    luminance = (0.299 * r + 0.587 * g + 0.114 * b) / 255
    return '#000000' if luminance > 0.5 else '#FFFFFF'

def display_lineup(lineup_data, team_name):
    """Displays the starting lineup for a team."""
    st.subheader(f"{team_name} Starters")

    # Data is already parsed from JSON by the DB driver
    if not lineup_data or not isinstance(lineup_data, list):
        st.write("Lineup data not available.")
        return

    starters = [p for p in lineup_data if p.get('IsStarter')]

    if not starters:
        st.write("No starters listed.")
        return

    cols = st.columns(4)
    for i, player in enumerate(starters):
        with cols[i % 4]:
            # Use .get() for safer dictionary access
            st.image(player.get('PlayerImageUrl', ''), width=60)
            st.caption(player.get('PlayerName', 'N/A'))

def run():
    """Main function to run the Match Facts Dashboard."""
    st.header("Match Facts Dashboard", anchor=False)
    
    df = load_match_facts_data()
    if df.empty:
        st.warning("No data found. Check connection and 'vw_match_facts' view.")
        st.stop()
        
    # --- Filters ---
    st.sidebar.header("📋 Match Filters")
    seasons = sorted(df['seasonname'].dropna().unique(), reverse=True)
    selected_season = st.sidebar.selectbox("Select Season", seasons)
    
    season_df = df[df['seasonname'] == selected_season].copy()
    
    # Match Round Filter
    match_rounds = ["All"] + sorted(season_df['matchround'].dropna().unique())
    selected_round = st.sidebar.selectbox("Select Match Round", match_rounds)
    
    # Start with the full season data, then narrow down
    filtered_df = season_df.copy()
    if selected_round != "All":
        filtered_df = filtered_df[filtered_df['matchround'] == selected_round]

    # Dynamic Home & Away Team Filters
    home_teams = ["All"] + sorted(filtered_df['hometeamname'].unique())
    selected_home_team = st.sidebar.selectbox("Select Home Team", home_teams)
    
    # If a home team is selected, filter the away team options
    if selected_home_team != "All":
        filtered_df = filtered_df[filtered_df['hometeamname'] == selected_home_team]

    away_teams = ["All"] + sorted(filtered_df['awayteamname'].unique())
    selected_away_team = st.sidebar.selectbox("Select Away Team", away_teams)

    # If an away team is selected, filter the dataframe further
    if selected_away_team != "All":
        filtered_df = filtered_df[filtered_df['awayteamname'] == selected_away_team]

    # --- Final Match Selection ---
    match_options = filtered_df['matchname'].unique()
    if len(match_options) == 1:
        selected_match_name = match_options[0]
        st.sidebar.info(f"Auto-selected match: {selected_match_name}")
    else:
        selected_match_name = st.sidebar.selectbox("Select Match", match_options)
    
    match_data = filtered_df[filtered_df['matchname'] == selected_match_name]
    if match_data.empty:
        st.warning("Selected match not found. Try adjusting filters.")
        st.stop()

    # --- Match Header ---
    home_team, score, away_team = st.columns([1, 0.5, 1])
    with home_team:
        st.image(match_data['hometeamimageurl'].iloc[0], width=80)
        st.subheader(match_data['hometeamname'].iloc[0])
    with score:
        st.title(f"{match_data['homegoals'].iloc[0]} - {match_data['awaygoals'].iloc[0]}")
    with away_team:
        st.image(match_data['awayteamimageurl'].iloc[0], width=80)
        st.subheader(match_data['awayteamname'].iloc[0])

    st.divider()

    # --- Main Content (Momentum & Stats) ---
    col1, col2 = st.columns(2)
    with col1:
        fig = create_momentum_chart(match_data)
        if fig:
            st.pyplot(fig)
        else:
            st.info("Momentum data not available for this match.")
    
    with col2:
        st.subheader("Top Stats")
        
        # --- Custom Possession Bar ---
        home_poss = int(match_data['homepossession'].iloc[0])
        away_poss = int(match_data['awaypossession'].iloc[0])
        home_color = match_data['hometeamcolor'].iloc[0] or '#d3151e'
        away_color = match_data['awayteamcolor'].iloc[0] or '#4a72d4'
        
        home_text_color = get_contrasting_text_color(home_color)
        away_text_color = get_contrasting_text_color(away_color)

        st.text("Ball Possession")
        possession_bar_html = f"""
            <div style="background: {away_color}; border-radius: 10px; height: 30px; width: 100%; display: flex; align-items: center;">
                <div style="background: {home_color}; width: {home_poss}%; height: 100%; border-radius: 10px 0 0 10px; display: flex; align-items: center; justify-content: center; color: {home_text_color}; font-weight: bold;">
                    {home_poss}%
                </div>
                <div style="width: {away_poss}%; height: 100%; display: flex; align-items: center; justify-content: center; color: {away_text_color}; font-weight: bold;">
                    {away_poss}%
                </div>
            </div>
            """
        st.markdown(possession_bar_html, unsafe_allow_html=True)
        st.write("") # Add a little space

        # Other stats
        stats = {
            "Expected goals (xG)": (match_data['homeexpectedgoals'].iloc[0], match_data['awayexpectedgoals'].iloc[0]),
            "Total shots": (match_data['homeshots'].iloc[0], match_data['awayshots'].iloc[0])
        }
        for stat, values in stats.items():
            c1, c2, c3 = st.columns([1, 0.5, 1])
            c1.metric(label="", value=f"{values[0]:.2f}" if isinstance(values[0], float) else values[0])
            c2.text(stat)
            c3.metric(label="", value=f"{values[1]:.2f}" if isinstance(values[1], float) else values[1])

    st.divider()

    # --- Lineups ---
    with st.expander("Show Starting Lineups"):
        col1, col2 = st.columns(2)
        with col1:
            display_lineup(match_data['homelineup'].iloc[0], match_data['hometeamname'].iloc[0])
        with col2:
            display_lineup(match_data['awaylineup'].iloc[0], match_data['awayteamname'].iloc[0]) 