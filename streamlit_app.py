import streamlit as st
import requests
import io
import pandas as pd

st.set_page_config(
    page_title="PitchingWRX Reports",
    page_icon="baseball",
    layout="centered"
)

API_URL = "https://web-production-12490.up.railway.app"
LOGO_URL = "https://raw.githubusercontent.com/pitchingwrx/pitchingwrx-reports/main/pwrx_logo.png"

col1, col2 = st.columns([1, 3])
with col1:
    st.image(LOGO_URL, width=160)
with col2:
    st.title("PitchingWRX")
    st.caption("Game Outing Report Generator")
st.markdown("---")

tab1, tab2, tab3 = st.tabs(["Generate Report", "Upload to Database", "Database Roster"])

# ── TAB 1: Generate Report ─────────────────────────────────────────────────────
with tab1:
    st.markdown("### Generate from Database")
    st.caption("Select a player and game already loaded in the database.")

    try:
        roster_resp = requests.get(API_URL + "/roster", timeout=10)
        roster_data = roster_resp.json().get("roster", [])
    except Exception:
        roster_data = []

    if roster_data:
        player_names = [r["player"] for r in roster_data]
        selected_player = st.selectbox("Select Player", player_names, key="gen_player")

        if selected_player:
            try:
                games_resp = requests.get(
                    API_URL + "/player_games",
                    params={"player": selected_player},
                    timeout=10
                )
                games = games_resp.json().get("games", [])
            except Exception:
                games = []

            if games:
                options = {g["label"]: g["date"] for g in games}
                selected_label = st.selectbox("Select Game", list(options.keys()), key="gen_game")
                selected_date = options[selected_label]

                if st.button("Generate Report", type="primary"):
                    with st.spinner("Generating report... ~30 seconds"):
                        try:
                            response = requests.post(
                                API_URL + "/generate_from_db",
                                data={
                                    "player_name": selected_player,
                                    "game_date": selected_date
                                },
                                timeout=120
                            )
                            if response.status_code == 200:
                                st.success("Report ready!")
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
                st.warning("No games found for " + selected_player)
    else:
        st.info("No players in database yet. Upload a file in the Upload tab first.")

    st.markdown("---")
    st.markdown("### Or Generate from File Upload")
    st.caption("Upload an XLSX file to generate a report and add data to the database.")

    uploaded_file = st.file_uploader(
        "Select a Trackman XLSX file",
        type=["xlsx"],
        key="gen_file"
    )

    if uploaded_file:
        st.success("File loaded: " + uploaded_file.name)

        with st.spinner("Reading file..."):
            try:
                resp = requests.post(
                    API_URL + "/games",
                    files={"file": (uploaded_file.name,
                                    uploaded_file.getvalue(),
                                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
                )
                all_games = resp.json().get("games", [])
            except Exception as e:
                st.error("Could not read file: " + str(e))
                all_games = []

        if all_games:
            players = list(dict.fromkeys(g["player"] for g in all_games if g.get("player")))
            selected_player_f = st.selectbox("Select Player", players, key="file_player")
            player_games = [g for g in all_games if g.get("player") == selected_player_f]

            if player_games:
                options_f = {g["label"]: g["date"] for g in player_games}
                selected_label_f = st.selectbox("Select Game", list(options_f.keys()), key="file_game")
                selected_date_f = options_f[selected_label_f]

                if st.button("Generate Report from File"):
                    with st.spinner("Generating report... ~30 seconds"):
                        try:
                            response = requests.post(
                                API_URL + "/generate",
                                files={"file": (uploaded_file.name,
                                                uploaded_file.getvalue(),
                                                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
                                data={"game_date": selected_date_f, "player_name": selected_player_f},
                                timeout=120
                            )
                            if response.status_code == 200:
                                st.success("Report ready!")
                                safe_name = selected_player_f.replace(" ", "_")
                                st.download_button(
                                    label="Download PDF Report",
                                    data=io.BytesIO(response.content),
                                    file_name=f"{safe_name}_{selected_date_f}.pdf",
                                    mime="application/pdf"
                                )
                            else:
                                st.error("Error: " + response.text)
                        except Exception as e:
                            st.error("Connection error: " + str(e))


# ── TAB 2: Upload to Database ──────────────────────────────────────────────────
with tab2:
    st.markdown("### Bulk Upload to Database")
    st.caption("Upload any XLSX file to store all pitches in the database. Single game, full season, or multi-player files all work.")

    upload_file = st.file_uploader(
        "Select a Trackman XLSX file",
        type=["xlsx"],
        key="bulk_upload"
    )

    if upload_file:
        st.success("File loaded: " + upload_file.name)

        if st.button("Upload to Database", type="primary"):
            with st.spinner("Uploading to database... please wait"):
                try:
                    response = requests.post(
                        API_URL + "/ingest",
                        files={"file": (upload_file.name,
                                        upload_file.getvalue(),
                                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
                        timeout=300
                    )
                    if response.status_code == 200:
                        result = response.json()
                        flagged = result.get("flagged", 0)
                        st.success(
                            f"Done! {result['inserted']} pitches added, "
                            f"{result['skipped']} already existed."
                            + (f" {flagged} rows flagged for review." if flagged else "")
                        )
                        warnings = result.get("warnings", [])
                        real_warnings = [w for w in warnings if w.startswith("WARNING")]
                        if real_warnings:
                            with st.expander(f"Data quality notes ({len(real_warnings)})"):
                                for w in real_warnings:
                                    st.warning(w)
                        summary = result.get("summary", [])
                        if summary:
                            st.markdown("**Players ingested:**")
                            summary_df = pd.DataFrame(summary)
                            summary_df.columns = ["Player", "Games", "Pitches"]
                            st.dataframe(summary_df, use_container_width=True, hide_index=True)
                    else:
                        st.error("Error: " + response.text)
                except Exception as e:
                    st.error("Connection error: " + str(e))


# ── TAB 3: Database Roster ─────────────────────────────────────────────────────
with tab3:
    st.markdown("### Players in Database")

    if st.button("Refresh Roster"):
        st.rerun()

    try:
        roster_resp2 = requests.get(API_URL + "/roster", timeout=10)
        roster_data2 = roster_resp2.json().get("roster", [])
    except Exception:
        roster_data2 = []

    if roster_data2:
        df_roster = pd.DataFrame(roster_data2)
        df_roster.columns = ["Player", "Games", "Pitches", "First Game", "Last Game"]
        st.dataframe(df_roster, use_container_width=True, hide_index=True)
        st.caption(f"{len(roster_data2)} players · {df_roster['Pitches'].sum():,} total pitches")
    else:
        st.info("No data in database yet.")

st.markdown("---")
st.caption("PitchingWRX - Data Driven Pitching Instruction")
