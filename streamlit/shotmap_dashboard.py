import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
from matplotlib.colors import LinearSegmentedColormap
from mplsoccer import VerticalPitch, FontManager, add_image
from db_connection import postgres_connection
import requests
from PIL import Image
import io

# Load custom font
fm_rubik = FontManager('https://raw.githubusercontent.com/google/fonts/main/ofl/rubikmonoone/RubikMonoOne-Regular.ttf')

@st.cache_data
def load_shotmap_data():
    """Loads shotmap data from the vw_player_shotmap view."""
    try:
        engine = postgres_connection()
        with engine.connect() as conn:
            query = "SELECT * FROM vw_player_shotmap;"
            df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.error(f"Database error while loading shotmap data: {e}")
        return pd.DataFrame()

def run():
    """Main function to run the Shotmap Dashboard."""
    st.header("Player Shot Map Analysis", anchor=False)
    
    df = load_shotmap_data()
    if df.empty:
        st.warning("No shotmap data loaded. Please check the database connection and the 'vw_player_shotmap' view.")
        st.stop()

    # --- Sidebar Filters ---
    st.sidebar.header("🎯 Shot Map Filters")
    player_search = st.sidebar.text_input("🔍 Search Player", "")
    
    if player_search:
        player_options = sorted(df[df['playername'].str.contains(player_search, case=False, na=False)]['playername'].unique())
    else:
        player_options = sorted(df['playername'].dropna().unique())

    if not player_options:
        st.sidebar.warning("No players found with that name.")
        st.stop()
        
    player_filter = st.sidebar.selectbox("Select Player", player_options)
    
    player_df = df[df['playername'] == player_filter]

    all_event_types = sorted(player_df['eventtype'].dropna().unique())
    # Set 'Goal' as default if available
    default_events = ['Goal'] if 'Goal' in all_event_types else all_event_types
    selected_event_types = st.sidebar.multiselect("Event Types", options=all_event_types, default=default_events)

    # Determine opponent teams for the selected player
    opponent_teams = sorted(pd.concat([
        player_df['hometeamname'], 
        player_df['awayteamname']
    ]).dropna().unique())
    
    # Filter out the player's own team from the opponent list
    player_team = player_df['playerteamname'].iloc[0] if not player_df.empty else None
    if player_team and player_team in opponent_teams:
        opponent_teams.remove(player_team)
        
    opponent_filter = st.sidebar.selectbox("Opponent Team (optional)", ["All"] + opponent_teams)

    seasons = sorted(player_df['seasonname'].dropna().unique())
    season_filter = st.sidebar.selectbox("Season (optional)", ["All"] + seasons)

    # --- Data Filtering Logic ---
    filtered_df = player_df.copy()
    if selected_event_types:
        filtered_df = filtered_df[filtered_df['eventtype'].isin(selected_event_types)]
    
    if opponent_filter != "All":
        # A match is against an opponent if the opponent is either the home or away team
        filtered_df = filtered_df[
            (filtered_df['hometeamname'] == opponent_filter) | 
            (filtered_df['awayteamname'] == opponent_filter)
        ]

    if season_filter != "All":
        filtered_df = filtered_df[filtered_df['seasonname'] == season_filter]

    if filtered_df.empty:
        st.warning("No shots found for the selected filters.")
        st.stop()

    # --- Plotting ---
    pitch = VerticalPitch(pad_bottom=1, half=True, goal_type='box', goal_alpha=0.8,
                          pitch_type='uefa', pitch_length=99.5, pitch_width=100)
    fig, ax = pitch.draw(figsize=(12, 10))
    # fig.set_facecolor('#22312b')
    # ax.patch.set_facecolor('#22312b')
    fig.set_facecolor('black')
    ax.patch.set_facecolor('black')


    # --- KDE Heatmap 
    if not filtered_df.empty:
        pitch.kdeplot(
            filtered_df['xposition'],
            filtered_df['yposition'],
            ax=ax,
            fill=True,
            levels=100,
            thresh=0.1,
            cut=4,
            cmap='Blues',
            alpha=0.3,  # Make it semi-transparen
            zorder=1      # Ensure it's above pitch lines but below shots
        )

    # Add player image to the top-left
    if not player_df.empty:
        player_image_url = player_df['playerimageurl'].iloc[0]
        if player_image_url:
            try:
                # Manually fetch and open the image to prevent errors
                response = requests.get(player_image_url, timeout=10)
                response.raise_for_status() # Raise an exception for bad status codes
                player_image = Image.open(io.BytesIO(response.content))
                
                ax_image = add_image(
                    player_image, fig, left=0.08, bottom=0.9, width=0.12, height=0.12
                )
            except Exception as e:
                st.warning(f"Could not load player image: {e}")

    # Define markers and colors for different event types
    event_types_unique = sorted(filtered_df['eventtype'].dropna().unique())
    
    # Use a color palette
    color_palette = plt.cm.get_cmap('tab10', len(event_types_unique))
    event_type_colors = {event: color_palette(i) for i, event in enumerate(event_types_unique)}
    
    # Use different markers
    markers = ['s', '^', 'D', 'P', '*', 'X', 'v', '<', '>']
    event_type_markers = {event: markers[i % len(markers)] for i, event in enumerate(event_types_unique)}

    # Plot shots by event type
    for event_type in event_types_unique:
        sub_df = filtered_df[filtered_df['eventtype'] == event_type]
        face_color = event_type_colors[event_type]
        marker = event_type_markers[event_type]
        
        # Override for goals to make them stand out
        if event_type == 'Goal':
            face_color = '#00FF00'
            marker = 'o'
            
        pitch.scatter(
            sub_df['xposition'],
            sub_df['yposition'],
            s=(sub_df['expectedgoals'] * 720) ,
            c=[face_color], # Pass color as a list
            edgecolors='#303030',
            linewidths=1.5,
            marker=marker,
            ax=ax,
            alpha=0.8,
            zorder=2 # Higher zorder to be on top of the heatmap
        )

        

    # --- Title and Subtitle ---
    total_shots = filtered_df.shape[0]
    total_goals = filtered_df[filtered_df['isgoal'] == True].shape[0]
    total_xg = filtered_df['expectedgoals'].sum()
    xg_per_shot = total_xg / total_shots if total_shots > 0 else 0

    title_text = f"{player_filter} Shot Map"
    stats_line = f"Shots: {total_shots} | Goals: {total_goals}  \nxG: {total_xg:.2f} | xG/Shot: {xg_per_shot:.2f}"
    
    subtitle_parts = []
    if season_filter != "All":
        subtitle_parts.append(f"Season:{season_filter}")
    if opponent_filter != "All":
        subtitle_parts.append(f"vs {opponent_filter}")
    subtitle = " | ".join(subtitle_parts)

    fig.text(0.5, 0.99, title_text, ha='center', va='top', fontproperties=fm_rubik.prop, fontsize=20, color='white')
    fig.text(0.5, 0.3, stats_line, ha='center', va='top', fontproperties=fm_rubik.prop, fontsize=15, color='white')
    if subtitle:
        fig.text(0.5, 0.95, subtitle, ha='center', va='top', fontproperties=fm_rubik.prop, fontsize=15, color='white')

    # --- Dynamic Legend ---
    # Legend for Event Types
    event_handles = []
    for event_type in event_types_unique:
        color = event_type_colors[event_type]
        marker = event_type_markers[event_type]
        if event_type == 'Goal':
            color = '#00FF00'
            marker = 'o'
        handle = mlines.Line2D([], [], color=color, marker=marker, linestyle='None',
                               markersize=10, label=event_type)
        event_handles.append(handle)

    event_legend = ax.legend(handles=event_handles, loc='upper right', 
                             title='Event Types', frameon=True, fontsize=10,
                             bbox_to_anchor=(1, 1.1),
                             labelcolor='black', title_fontsize=12,
                             facecolor='white',
                             edgecolor='black')
    ax.add_artist(event_legend)

    # Legend for xG (size of markers)
    xg_sizes = [0.1, 0.3, 0.5, 0.7, 0.9]
    xg_handles = []
    for xg in xg_sizes:
        size = (xg * 720) 
        handle = mlines.Line2D([], [], color='black', marker='o', linestyle='None',
                               markersize=size**0.4, label=f'xG: {xg}')
        xg_handles.append(handle)
    
    xg_legend = ax.legend(handles=xg_handles, loc='upper right',
                         title='Expected Goals (xG)', frameon=True, fontsize=10,
                         bbox_to_anchor=(1, 0.9),
                         labelcolor='black', title_fontsize=12,
                         facecolor='white',
                         edgecolor='black')
    ax.add_artist(xg_legend)

    # plt.tight_layout()

    st.pyplot(fig) 