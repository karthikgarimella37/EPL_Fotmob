import streamlit as st
import shotmap_dashboard
import comparison_dashboard
import match_facts_dashboard

# Set the page configuration for the entire app
st.set_page_config(
    page_title="EPL Dashboard",
    page_icon="⚽",
    layout="centered",
    initial_sidebar_state="collapsed"
)

# --- App Navigation ---
PAGES = {
    "Shot Map Dashboard": shotmap_dashboard,
    "Player Comparison Pizza Plots": comparison_dashboard,
    "Match Facts Dashboard": match_facts_dashboard
}

st.sidebar.title('Select Dashboard to view')
selection = st.sidebar.radio("Go to", list(PAGES.keys()))
page = PAGES[selection]

def main():
    """
    Main function to orchestrate the Streamlit application.
    This includes a navigator for multiple dashboards.
    """
    page.run()

if __name__ == "__main__":
    main() 