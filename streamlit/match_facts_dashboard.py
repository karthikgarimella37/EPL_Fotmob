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
    momentum_data = match_data['momentum'].iloc[0]
    # The actual data is nested one level deeper under the 'data' key
    if not momentum_data or 'main' not in momentum_data or 'data' not in momentum_data['main']:
        return None, None

    # Create the DataFrame from the correct path
    df_momentum = pd.DataFrame(momentum_data['main']['data'])
    
    if df_momentum.empty or 'value' not in df_momentum.columns:
        return None, None

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

def get_contrasting_text_color(hex_color):
    """Returns black or white for text depending on the background hex color's brightness."""
    if not hex_color:
        return '#FFFFFF'
    hex_color = hex_color.lstrip('#')
    r, g, b = tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))
    # Calculate luminance
    luminance = (0.299 * r + 0.587 * g + 0.114 * b) / 255
    return '#000000' if luminance > 0.5 else '#FFFFFF'

def display_lineup(lineup_json, team_name):
    """Displays the starting lineup for a team."""
    st.subheader(f"{team_name} Lineup")
    lineup = json.loads(lineup_json)
    starters = [p for p in lineup if p['IsStarter']]
    
    cols = st.columns(4)
    for i, player in enumerate(starters):
        with cols[i % 4]:
            st.image(player['PlayerImageUrl'], width=60)
            st.caption(player['PlayerName'])

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
    
    season_matches = df[df['seasonname'] == selected_season]
    match_options = season_matches['matchname'].unique()
    selected_match_name = st.sidebar.selectbox("Select Match", match_options)
    
    match_data = season_matches[season_matches['matchname'] == selected_match_name]
    if match_data.empty:
        st.warning("Selected match not found.")
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