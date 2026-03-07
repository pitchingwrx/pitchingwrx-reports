import streamlit as st
import requests
import io

st.set_page_config(
    page_title="PitchingWRX Reports",
    page_icon="⚾",
    layout="centered"
)

st.title("PitchingWRX")
st.subheader("Game Outing Report Generator")
st.markdown("---")

API_URL = "https://web-production-12490.up.railway.app"

st.markdown("### Upload Game File")
uploaded_file = st.file_uploader(
    "Select a Trackman XLSX file",
    type=["xlsx"],
    help="Upload a single game or full season XLSX export from Trackman"
)

if uploaded_file is not None:
    st.success("File loaded: " + uploaded_file.name)

    with st.spinner("Reading games from file..."):
        try:
            resp = requests.post(
                API_URL + "/games",
                files={"file": (uploaded_file.name,
                                uploaded_file.getvalue(),
                                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
            )
            games_data = resp.json()
            games = games_data.get("games", [])
        except Exception as e:
            st.error("Could not read games: " + str(e))
            games = []

    if games:
        options = {g["label"]: g["date"] for g in games}
        selected_label = st.selectbox("Select Game to Report", list(options.keys()))
        selected_date = options[selected_label]

        st.caption(f"Selected date: {selected_date}")

        if st.button("Generate Report"):
            with st.spinner("Generating report... this takes about 30 seconds"):
                try:
                    response = requests.post(
                        API_URL + "/generate",
                        files={"file": (uploaded_file.name,
                                        uploaded_file.getvalue(),
                                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
                        data={"game_date": selected_date}
                    )

                    if response.status_code == 200:
                        st.success("Report generated!")
                        st.download_button(
                            label="Download PDF Report",
                            data=io.BytesIO(response.content),
                            file_name=f"report_{selected_date}.pdf",
                            mime="application/pdf"
                        )
                    else:
                        st.error("Error: " + response.text)

                except Exception as e:
                    st.error("Connection error: " + str(e))
    else:
        st.warning("No games found in file.")

st.markdown("---")
st.caption("PitchingWRX - Data Driven Pitching Instruction")
