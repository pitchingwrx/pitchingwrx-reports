import streamlit as st
import requests
import io

st.set_page_config(
    page_title="PitchingWRX Reports",
    page_icon="baseball",
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
    help="Upload a single game, single player, or full multi-player season file"
)

if uploaded_file is not None:
    st.success("File loaded: " + uploaded_file.name)

    with st.spinner("Reading file..."):
        try:
            resp = requests.post(
                API_URL + "/games",
                files={"file": (uploaded_file.name,
                                uploaded_file.getvalue(),
                                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
            )
            data = resp.json()
            all_games = data.get("games", [])
        except Exception as e:
            st.error("Could not read file: " + str(e))
            all_games = []

    if all_games:
        # Build player list (unique names)
        players = list(dict.fromkeys(g["player"] for g in all_games if g.get("player")))

        if len(players) > 1:
            selected_player = st.selectbox("Select Player", players)
        else:
            selected_player = players[0] if players else None
            if selected_player:
                st.info("Player: " + selected_player)

        # Filter games to selected player
        player_games = [g for g in all_games if g.get("player") == selected_player]

        if player_games:
            options = {g["label"]: g["date"] for g in player_games}
            selected_label = st.selectbox("Select Game", list(options.keys()))
            selected_date = options[selected_label]

            if st.button("Generate Report"):
                with st.spinner("Generating report... this takes about 30 seconds"):
                    try:
                        response = requests.post(
                            API_URL + "/generate",
                            files={"file": (uploaded_file.name,
                                            uploaded_file.getvalue(),
                                            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
                            data={
                                "game_date": selected_date,
                                "player_name": selected_player
                            }
                        )

                        if response.status_code == 200:
                            st.success("Report generated!")
                            safe_name = selected_player.replace(" ", "_")
                            st.download_button(
                                label="Download PDF Report",
                                data=io.BytesIO(response.content),
                                file_name=f"{safe_name}_{selected_date}.pdf",
                                mime="application/pdf"
                            )
                        else:
                            st.error("Error: " + response.text)

                    except Exception as e:
                        st.error("Connection error: " + str(e))
        else:
            st.warning("No games found for selected player.")
    else:
        st.warning("No games found in file.")

st.markdown("---")
st.caption("PitchingWRX - Data Driven Pitching Instruction")
