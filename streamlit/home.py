import streamlit as st
from PIL import Image

def run():
    st.title("Welcome to the EPL Analytics Hub!")

    st.markdown("""
        This application provides in-depth analysis and visualizations for the English Premier League (EPL).
        
        **Please select a dashboard from the sidebar to get started on the top left.**
        
        Explore match statistics, player performance, and tactical insights across different seasons.
    """)
    
    st.divider()

    st.header("What You Can Do:")
    
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Analyze Match Facts")
        st.markdown("""
            Dive deep into individual matches. View key stats, momentum charts, starting lineups, 
            xG race plots, and detailed shot maps for any game.
        """)
        try:
            image = Image.open("images/Match Facts Dashboard.png")
            st.image(image, caption="Match Facts & Momentum", use_container_width=True)
            image1 = Image.open("images/xG Race Plot.png")
            st.image(image1, caption="xG Race Plot", use_container_width=True)
        except FileNotFoundError:
            st.warning("Could not find image: Match Facts Dashboard.png or xG Race Plot.png")

    with col2:
        st.subheader("Compare Players")
        st.markdown("""
            Compare two players head-to-head using detailed pizza plots. 
            Select from a wide range of statistics to see who comes out on top.
        """)
        try:
            image = Image.open("images/Mo Salah v Haaland Pizza Plot.png")
            st.image(image, caption="Player Comparison Pizza Plot", use_container_width=True)
        except FileNotFoundError:
            st.warning("Could not find image: Mo Salah v Haaland Pizza Plot.png")
            
    st.header("Visualize Shot Data")
    st.markdown("""
        Analyze team and player shot maps for any match. See where shots were taken from, 
        their quality (xG), and which ones resulted in goals.
    """)
    try:
        image = Image.open("images/Mo Salah Shotmap.png")
        st.image(image, caption="Team Shotmap", use_container_width=True)
    except FileNotFoundError:
        st.warning("Could not find image: Mo Salah Shotmap.png") 