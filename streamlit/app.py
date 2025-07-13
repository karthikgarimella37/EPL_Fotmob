import streamlit as st
import shotmap_dashboard
import comparison_dashboard
import match_facts_dashboard

# Set the page configuration for the entire app
st.set_page_config(
    page_title="EPL Fotmob Analytics",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- App Navigation ---
PAGES = {
    "Shot Map Analysis": shotmap_dashboard,
    "Player Comparison": comparison_dashboard,
    "Match Facts": match_facts_dashboard
}

st.sidebar.title('Dashboard Navigation')
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