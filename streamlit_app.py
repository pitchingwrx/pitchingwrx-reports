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
    help="Upload the game XLSX export from Trackman"
)

if uploaded_file is not None:
    st.success("File loaded: " + uploaded_file.name)

    if st.button("Generate Report"):
        with st.spinner("Generating report... this takes about 30 seconds"):
            try:
                response = requests.post(
                    API_URL + "/generate",
                    files={"file": (uploaded_file.name,
                                    uploaded_file.getvalue(),
                                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
                )

                if response.status_code == 200:
                    pdf_bytes = response.content
                    filename = "report.pdf"

                    st.success("Report generated successfully!")
                    st.download_button(
                        label="Download PDF Report",
                        data=io.BytesIO(pdf_bytes),
                        file_name=filename,
                        mime="application/pdf"
                    )
                else:
                    st.error("Error: " + response.text)

            except Exception as e:
                st.error("Connection error: " + str(e))

st.markdown("---")
st.caption("PitchingWRX - Data Driven Pitching Instruction")
