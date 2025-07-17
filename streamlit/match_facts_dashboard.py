import streamlit as st
import pandas as pd
import json
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from db_connection import postgres_connection
from mplsoccer import Pitch
import numpy as np

@st.cache_data
def load_match_facts_data():
    """Loads match facts data from the database."""
    try:
        engine = postgres_connection()
        with engine.connect() as conn:
            query = "SELECT * FROM vw_match_facts where matchqaanswer is not null;"
            df = pd.read_sql(query, conn)
        # Convert momentum from string to dict
        df['momentum'] = df['momentum'].apply(lambda x: json.loads(x) if x else None)
        return df
    except Exception as e:
        st.error(f"Database error loading match facts: {e}")
        return pd.DataFrame()

@st.cache_data
def load_player_match_analytics(match_id):
    """Loads player match analytics for a given match_id."""
    if not match_id:
        return pd.DataFrame()
    try:
        engine = postgres_connection()
        with engine.connect() as conn:
            # Use parameterized query to prevent SQL injection
            query = "SELECT * FROM vw_player_match_analytics WHERE matchid = %(match_id)s;"
            params = {'match_id': int(match_id)}
            df = pd.read_sql(query, conn, params=params)
        return df
    except Exception as e:
        st.error(f"Database error loading player match analytics: {e}")
        return pd.DataFrame()

@st.cache_data
def load_shotmap_data(match_id):
    """Loads shotmap data for a given match_id from the vw_player_shotmap view."""
    if not match_id:
        return pd.DataFrame()
    try:
        engine = postgres_connection()
        with engine.connect() as conn:
            query = "SELECT * FROM vw_player_shotmap WHERE matchid = %(match_id)s;"
            params = {'match_id': int(match_id)}
            df = pd.read_sql(query, conn, params=params)
        return df
    except Exception as e:
        st.error(f"Database error loading shotmap data: {e}")
        return pd.DataFrame()

def create_momentum_chart(match_data, shotmap_df):
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

        # Get the y-limits to place goal markers at the top/bottom
        ymin, ymax = ax.get_ylim()
        
        ax.set_xticks([0, len(df_momentum) / 2, len(df_momentum)])
        ax.set_xticklabels(["0'", 'HT', 'FT'], color='white', fontsize=12)
        ax.set_yticks([])
        
        for spine in ax.spines.values():
            spine.set_visible(False)
            
        ax.set_title('Momentum', color='white', fontsize=16)
        
        # --- Add goal markers ---
        if not shotmap_df.empty:
            home_team_id = match_data['hometeamid'].iloc[0]
            away_team_id = match_data['awayteamid'].iloc[0]
            
            goals = shotmap_df[shotmap_df['isgoal'] == True]
            home_goals = goals[goals['playerteamid'] == home_team_id]
            away_goals = goals[goals['playerteamid'] == away_team_id]
            
            num_points = len(df_momentum)
            # Assuming momentum data covers ~95 minutes of match time for scaling
            total_match_minutes = 95.0
            
            for _, goal in home_goals.iterrows():
                goal_minute = goal['minute']
                x_pos = int(goal_minute * num_points / total_match_minutes)
                if 0 <= x_pos < num_points:
                    ax.plot(x_pos, ymax * 0.9, marker='*', color='yellow', markersize=12,
                            markeredgecolor=home_color, markeredgewidth=1, clip_on=False)

            for _, goal in away_goals.iterrows():
                goal_minute = goal['minute']
                x_pos = int(goal_minute * num_points / total_match_minutes)
                if 0 <= x_pos < num_points:
                    ax.plot(x_pos, ymin * 0.9, marker='*', color='yellow', markersize=12,
                            markeredgecolor=away_color, markeredgewidth=1, clip_on=False)
        
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
    
    # --- Help Section ---
    with st.expander("How to use this dashboard"):
        st.markdown("""
            This dashboard allows you to analyze match facts, shotmaps, xG race plots, and advanced team stats.
            
            **How it works:**
            1.  **Select a Season:** Choose the season you want to analyze from the dropdown menu.
            2.  **Select a Match Round:** (optional) Choose the match round you want to analyze from the dropdown menu.
            3.  **Select a Home Team:** (optional) Choose the home team you want to analyze from the dropdown menu.
            4.  **Select an Away Team:** (optional) Choose the away team you want to analyze from the dropdown menu.
            5.  **Select a Match:** Choose the match you want to analyze from the dropdown menu.
        """)

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
    match_rounds = ["Any"] + sorted(season_df['matchround'].dropna().unique().astype(int))
    selected_round = st.sidebar.selectbox("Select Match Round", match_rounds)
    
    # Start with the full season data, then narrow down
    filtered_df = season_df.copy()
    if selected_round != "Any":
        filtered_df = filtered_df[filtered_df['matchround'] == selected_round]

    # Dynamic Home & Away Team Filters
    home_teams = ["Any"] + sorted(filtered_df['hometeamname'].unique())
    selected_home_team = st.sidebar.selectbox("Select Home Team", home_teams)
    
    # If a home team is selected, filter the away team options
    if selected_home_team != "Any":
        filtered_df = filtered_df[filtered_df['hometeamname'] == selected_home_team]

    away_teams = ["Any"] + sorted(filtered_df['awayteamname'].unique())
    selected_away_team = st.sidebar.selectbox("Select Away Team", away_teams)

    # If an away team is selected, filter the dataframe further
    if selected_away_team != "Any":
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

    match_id = match_data['matchid'].iloc[0]
    player_stats_df = load_player_match_analytics(match_id)
    shotmap_df = load_shotmap_data(match_id)


    # --- Match Header ---
    st.divider()
    home_team, score, away_team = st.columns([1, 0.5, 1])
    with home_team:
        st.image(match_data['hometeamimageurl'].iloc[0], width=80)
        st.subheader(match_data['hometeamname'].iloc[0])
        if not shotmap_df.empty:
            home_team_id = match_data['hometeamid'].iloc[0]
            home_goals = shotmap_df[(shotmap_df['isgoal'] == True) & (shotmap_df['playerteamid'] == home_team_id)]
            if not home_goals.empty:
                scorers = home_goals.groupby('lastname')['minute'].apply(lambda x: sorted(list(x))).reset_index()
                for _, row in scorers.iterrows():
                    minutes_str = ", ".join([f"{m}'" for m in row['minute']])
                    st.caption(f"{row['lastname']} {minutes_str}")
    with score:
        st.title(f"{match_data['homegoals'].iloc[0]} - {match_data['awaygoals'].iloc[0]}")
    with away_team:
        st.image(match_data['awayteamimageurl'].iloc[0], width=80)
        st.subheader(match_data['awayteamname'].iloc[0])
        if not shotmap_df.empty:
            away_team_id = match_data['awayteamid'].iloc[0]
            away_goals = shotmap_df[(shotmap_df['isgoal'] == True) & (shotmap_df['playerteamid'] == away_team_id)]
            if not away_goals.empty:
                scorers = away_goals.groupby('lastname')['minute'].apply(lambda x: sorted(list(x))).reset_index()
                for _, row in scorers.iterrows():
                    minutes_str = ", ".join([f"{m}'" for m in row['minute']])
                    st.caption(f"{row['lastname']} {minutes_str}")

    


     # --- Match Details ---
    st.divider()
    col1, col2, col3 = st.columns(3)
    match_row = match_data.iloc[0]

    with col1:
        st.caption("Season")
        st.write(match_row.get('seasonname', "N/A"))
        st.caption("Stadium")
        st.write(match_row.get('stadiumname', "N/A"))

    with col2:
        st.caption("Date")
        match_time_utc = match_row.get('matchtimeutc')
        if match_time_utc:
            date_str = pd.to_datetime(match_time_utc).strftime('%d %b %Y, %H:%M')
            st.write(f"{date_str} UTC")
        else:
            st.write("N/A")
        
        st.caption("Attendance")
        attendance = match_row.get('attendance')
        attendance_str = f"{int(attendance):,}" if pd.notna(attendance) and attendance > 0 else "N/A"
        st.write(attendance_str)

    with col3:
        st.caption("Match Round")
        st.write(match_row.get('matchround', "N/A"))
        st.caption("Referee")
        st.write(match_row.get('refereename', "N/A"))

    st.divider()

    # --- Main Content (Momentum & Stats) ---
    col1, col2 = st.columns(2)
    with col1:
        fig = create_momentum_chart(match_data, shotmap_df)
        if fig:
            st.pyplot(fig)
            # --- Momentum Help Section ---
            if match_data['momentum'].iloc[0] is not None:
                match_momentum_url = "https://theanalyst.com/articles/what-is-match-momentum"
                with st.expander("What does the Momentum chart show?"):
                    st.markdown(f"""*[Match momentum](%s)* measures the swing of the match and 
                                which team is creating more threatening situations at certain points in time. 
                                It does this by measuring the likelihood of the team in possession scoring within
                                 the next 10 seconds. The star markers indicate goals scored by the team.""" % match_momentum_url)
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

        with st.container(border=True):
            home_team_name = match_data['hometeamname'].iloc[0]
            away_team_name = match_data['awayteamname'].iloc[0]

            for i, (stat, values) in enumerate(stats.items()):
                c1, c2, c3 = st.columns([1, 1.5, 1])
                home_val, away_val = values
                
                with c1:
                    st.metric(label=home_team_name, value=f"{home_val:.2f}" if isinstance(home_val, float) else home_val)
                with c2:
                    st.markdown(f"<div style='text-align: center; padding-top: 2.5rem;'>{stat}</div>", unsafe_allow_html=True)
                with c3:
                    st.metric(label=away_team_name, value=f"{away_val:.2f}" if isinstance(away_val, float) else away_val)

                if i < len(stats) - 1:
                    st.divider()

    st.divider()

    # --- Lineups ---
    with st.expander("Show Starting Lineups"):
        col1, col2 = st.columns(2)
        with col1:
            display_lineup(match_data['homelineup'].iloc[0], match_data['hometeamname'].iloc[0])
        with col2:
            display_lineup(match_data['awaylineup'].iloc[0], match_data['awayteamname'].iloc[0])

    # --- xG Race Plot ---
    with st.expander("View xG Race Plot"):
        if shotmap_df.empty:
            st.warning("Shotmap data not available for this match.")
        else:
            fig = create_xg_race_plot(shotmap_df, match_data)
            st.plotly_chart(fig, use_container_width=True)
            
            # --- xG Race Plot Help Section ---
            if not shotmap_df.empty:
                xg_race_url = "https://theanalyst.com/na/2021/07/what-is-expected-goals-xg/"
                with st.expander("What does the xG Race Plot show?"):
                    st.markdown(f"An **xG Race Plot** shows the cumulative total of *[Expected Goals (xG)]({xg_race_url})* for each team as the match progresses. It helps visualize which team created more high-quality scoring chances and illustrates the flow of the game in terms of attacking threat.")

    # --- Shot Map ---
    with st.expander("View Shot Map"):
        if shotmap_df.empty:
            st.warning("Shotmap data not available for this match.")
        else:
            fig = create_shot_map(shotmap_df, match_data)
            st.pyplot(fig, use_container_width=True)
            
            # --- Shot Map Help Section ---
            if not shotmap_df.empty:
                shot_map_url = "https://theanalyst.com/na/2021/07/what-is-expected-goals-xg/"
                with st.expander("What does the Shot Map show? and how to use it?"):
                    st.markdown(f"""
                        A **Shot Map** is a visual representation of every shot taken during a football match. 
                        Each dot corresponds to a shot's location on the pitch. The size of the dot represents the 
                        [Expected Goals (xG)]({shot_map_url}) value of the shot—the larger the dot, the higher the probability of it being a goal. 
                        Goals are highlighted with a star.
                    """)

    # --- Advanced Stats ---
    with st.expander("View Advanced Team Stats"):
        if player_stats_df.empty:
            st.warning("Advanced player stats are not available for this match.")
        else:
            home_team_id = match_data['hometeamid'].iloc[0]
            away_team_id = match_data['awayteamid'].iloc[0]
            home_color = match_data['hometeamcolor'].iloc[0] or '#d3151e'
            away_color = match_data['awayteamcolor'].iloc[0] or '#4a72d4'

            home_stats = player_stats_df[player_stats_df['teamid'] == home_team_id]
            away_stats = player_stats_df[player_stats_df['teamid'] == away_team_id]

            def stat_row(label, home_val, away_val, is_float=False):
                home_disp = f"{home_val:.2f}" if is_float else f"{int(home_val)}"
                away_disp = f"{away_val:.2f}" if is_float else f"{int(away_val)}"
                
                col1, col2, col3 = st.columns([1, 1.5, 1])
                with col1:
                    st.markdown(f"<p style='color:{home_color}; text-align:center; font-weight:bold;'>{home_disp}</p>", unsafe_allow_html=True)
                with col2:
                    st.markdown(f"<p style='text-align:center;'>{label}</p>", unsafe_allow_html=True)
                with col3:
                    st.markdown(f"<p style='color:{away_color}; text-align:center; font-weight:bold;'>{away_disp}</p>", unsafe_allow_html=True)

            # Define stats to display
            advanced_stats = {
                "Attacking": [
                    ("Expected Goals (xG)", "expectedgoals", True),
                    ("Expected Assists (xA)", "expectedassists", True),
                    ("Successful Dribbles", "successfuldribbles", False),
                    ("Touches in Opp. Box", "touchesinoppbox", False)
                ],
                "Passing": [
                    ("Accurate Passes", "accuratepasses", False),
                    ("Passes into Final Third", "passesintofinalthird", False),
                    ("Chances Created", "chancescreated", False)
                ],
                "Defending": [
                    ("Tackles Won", "tackleswon", False),
                    ("Interceptions", "interceptions", False),
                    ("Recoveries", "recoveries", False),
                    ("Duels Won", "duelswon", False),
                    ("Clearances", "clearances", False)
                ]
            }

            for category, stats_list in advanced_stats.items():
                st.subheader(category)
                for label, key, is_float in stats_list:
                    home_val = home_stats[key].sum()
                    away_val = away_stats[key].sum()
                    stat_row(label, home_val, away_val, is_float)
                st.divider() 


    with st.expander("Credits and Source Code"):
        st.caption("""
            This dashboard was created by Karthik Garimella using the [mplsoccer](https://mplsoccer.readthedocs.io/en/latest/gallery/pitch_plots/plot_scatter.html#sphx-glr-gallery-pitch-plots-plot-scatter-py/) library.
            The data is sourced from the [EPL Fotmob API](https://www.fotmob.com).
            The github repository for this project is [here](https://github.com/karthikgarimella37/EPL_Fotmob).
        """)

def create_xg_race_plot(shotmap_df, match_data):
    """Creates and returns a Plotly figure for the xG race plot."""
    
    home_team_id = match_data['hometeamid'].iloc[0]
    away_team_id = match_data['awayteamid'].iloc[0]
    home_team_name = match_data['hometeamname'].iloc[0]
    away_team_name = match_data['awayteamname'].iloc[0]
    home_color = match_data['hometeamcolor'].iloc[0] or '#d3151e'
    away_color = match_data['awayteamcolor'].iloc[0] or '#4a72d4'

    df = shotmap_df.copy()
    df = df.astype({"expectedgoals": float, "minute": int})

    # Separate home and away shots
    df_home_shots = df[df['playerteamid'] == home_team_id]
    df_away_shots = df[df['playerteamid'] == away_team_id]

    # Add a starting point at minute 0 with 0 xG for both teams
    start_row = pd.DataFrame([{'minute': 0, 'expectedgoals': 0.0}])
    df_home = pd.concat([start_row, df_home_shots]).sort_values('minute').reset_index(drop=True)
    df_away = pd.concat([start_row, df_away_shots]).sort_values('minute').reset_index(drop=True)

    # Calculate cumulative xG
    df_home['xgcum'] = df_home['expectedgoals'].cumsum()
    df_away['xgcum'] = df_away['expectedgoals'].cumsum()
    
    # Get goal events for annotations
    goals_home = df_home[df_home['isgoal'] == True]
    goals_away = df_away[df_away['isgoal'] == True]

    # Create figure
    fig = go.Figure()

    # Add step plots for cumulative xG
    fig.add_trace(go.Scatter(
        x=df_home['minute'], y=df_home['xgcum'], name=home_team_name,
        line_shape='hv', line=dict(color=home_color, width=3),
        mode='lines'
    ))
    fig.add_trace(go.Scatter(
        x=df_away['minute'], y=df_away['xgcum'], name=away_team_name,
        line_shape='hv', line=dict(color=away_color, width=3),
        mode='lines'
    ))

    # Add markers for goals
    fig.add_trace(go.Scatter(
        x=goals_home['minute'], y=goals_home['xgcum'],
        mode='markers+text',
        marker=dict(color=home_color, size=18, line=dict(color='white', width=1.5)),
        text=goals_home['expectedgoals'].round(2),
        textfont=dict(color='white', size=9),
        textposition="middle center",
        hoverinfo='text',
        hovertext=goals_home['playername'] + ' (' + goals_home['minute'].astype(str) + "')",
        showlegend=False
    ))
    fig.add_trace(go.Scatter(
        x=goals_away['minute'], y=goals_away['xgcum'],
        mode='markers+text',
        marker=dict(color=away_color, size=18,  line=dict(color='white', width=1.5)),
        text=goals_away['expectedgoals'].round(2),
        textfont=dict(color='black', size=9),
        textposition="middle center",
        hoverinfo='text',
        hovertext=goals_away['playername'] + ' (' + goals_away['minute'].astype(str) + "')",
        showlegend=False
    ))


    # Update layout
    home_xg_total = df_home['expectedgoals'].sum().round(2)
    away_xg_total = df_away['expectedgoals'].sum().round(2)
    home_goals = match_data['homegoals'].iloc[0]
    away_goals = match_data['awaygoals'].iloc[0]
    
    fig.update_layout(
        title=f"<b>{home_team_name} [{home_xg_total} xG] {home_goals} - {away_goals} {away_team_name} [{away_xg_total} xG]</b>",
        xaxis_title="Minute", yaxis_title="Cumulative xG",
        template='plotly_dark',
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01),
        xaxis=dict(range=[0, 95], tickvals=list(range(0, 91, 15))),
        yaxis=dict(range=[0, max(df_home['xgcum'].max(), df_away['xgcum'].max()) * 1.1]),
        shapes=[
            dict(type='line', x0=45, x1=45, y0=0, y1=max(df_home['xgcum'].max(), df_away['xgcum'].max()) * 1.1,
                 line=dict(color='white', width=1, dash='dash'))
        ],
        annotations=[
            dict(x=22.5, y=max(df_home['xgcum'].max(), df_away['xgcum'].max()) * 1.05, text="First Half", showarrow=False, font=dict(size=14)),
            dict(x=67.5, y=max(df_home['xgcum'].max(), df_away['xgcum'].max()) * 1.05, text="Second Half", showarrow=False, font=dict(size=14))
        ]
    )
    return fig 

def create_shot_map(shotmap_df, match_data):
    """Creates and returns a matplotlib figure for the team shot map."""

    home_team_id = match_data['hometeamid'].iloc[0]
    away_team_id = match_data['awayteamid'].iloc[0]
    home_color = match_data['hometeamcolor'].iloc[0] or '#d3151e'
    away_color = match_data['awayteamcolor'].iloc[0] or '#4a72d4'
    home_team_name = match_data['hometeamname'].iloc[0]
    away_team_name = match_data['awayteamname'].iloc[0]
    
    df = shotmap_df.copy()
    df = df.astype({"expectedgoals": float, "xposition": float, "yposition": float, "isgoal": bool})

    pitch = Pitch(pitch_type='uefa', pitch_color='black', line_color='white', line_zorder=2)
    fig, ax = pitch.draw(figsize=(10, 7))
    fig.set_facecolor("black")
    
    home_shots = df[(df['playerteamid'] == home_team_id) & (df['isgoal'] == False)]
    home_goals = df[(df['playerteamid'] == home_team_id) & (df['isgoal'] == True)]
    away_shots = df[(df['playerteamid'] == away_team_id) & (df['isgoal'] == False)]
    away_goals = df[(df['playerteamid'] == away_team_id) & (df['isgoal'] == True)]

    
    # Home team (attacking left to right) - plot y, x
    pitch.scatter(105 - home_shots.xposition, home_shots.yposition, s=home_shots.expectedgoals * 500,
                  c=home_color, marker='o', ax=ax)
    pitch.scatter(105 - home_goals.xposition,  home_goals.yposition, s=home_goals.expectedgoals * 500,
                  c=home_color, ec=away_color, marker='*', ax=ax, linewidth=0.5)

    # Away team (attacking right to left, flip coordinates)
    pitch.scatter( away_shots.xposition, away_shots.yposition, s=away_shots.expectedgoals * 500,
                  c=away_color, marker='o', ax=ax)
    pitch.scatter( away_goals.xposition, away_goals.yposition, s=away_goals.expectedgoals * 500,
                  c=away_color, ec=home_color, marker='*', ax=ax, linewidth=0.5)
    
    
    ax.set_title('')
    fig.text(0.5, 0.96, ' vs ', color='white', ha='center', va='center', fontsize=18)
    fig.text(0.48, 0.96, home_team_name, color=home_color, ha='right', va='center', fontsize=18, weight='bold')
    fig.text(0.52, 0.96, away_team_name, color=away_color, ha='left', va='center', fontsize=18, weight='bold')

    # xG legend (top)
    xg_legend_elements = []
    for xg_val in [0.1, 0.3, 0.5, 0.7, 0.9]:
        xg_legend_elements.append(plt.Line2D([0], [0], marker='o', color='w', label=f'{xg_val:.1f} xG',
                               markerfacecolor='grey', markersize=np.sqrt(xg_val * 500), linestyle='None'))
    
    xg_legend = ax.legend(handles=xg_legend_elements, loc='upper center', bbox_to_anchor=(0.51, 0.93),
               frameon=False, shadow=True, ncol=5, facecolor='black', labelcolor='white')
    plt.setp(xg_legend.get_title())
    ax.add_artist(xg_legend)

    # Main legend for teams and goal type (bottom)
    home_marker = plt.Line2D([0], [0], marker='o', color='w', label=home_team_name, 
                           markerfacecolor=home_color, markersize=10, linestyle='None')
    away_marker = plt.Line2D([0], [0], marker='o', color='w', label=away_team_name, 
                           markerfacecolor=away_color, markersize=10, linestyle='None')
    goal_marker = plt.Line2D([0], [0], marker='*', color='black', label='Goal', 
                           markerfacecolor='yellow', markersize=15, linestyle='None')
    
    ax.legend(handles=[home_marker, away_marker, goal_marker], loc='lower center', bbox_to_anchor=(0.5, -0.05),
              fancybox=True, shadow=True, ncol=3, facecolor='black', edgecolor='white', labelcolor='white')

    return fig 