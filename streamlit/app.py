import streamlit as st
import shotmap_dashboard

# Set the page configuration for the entire app
st.set_page_config(
    page_title="EPL Fotmob Analytics",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded"
)

def main():
    """
    Main function to orchestrate the Streamlit application.
    This can be expanded to include a navigator for multiple dashboards.
    """
    # For now, we directly run the shotmap dashboard.
    # In the future, you could have a sidebar to select different views.
    shotmap_dashboard.run()

if __name__ == "__main__":
    main() 