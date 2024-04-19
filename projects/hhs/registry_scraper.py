import requests
import re
from datetime import datetime, date
from bs4 import BeautifulSoup
import pandas as pd
from loguru import logger


def return_empty_row(case_number):
    empty_row = {
        "case_number": case_number,
        "court": "",
        "ward_name": "",
        "ward_type": "",
        "birth_year": "",
        "guardianship_type": "",
        "guardian(s)": "",
        "guardianship_scope": "",
        "issue_date": "",
        "expiration_date": "",
    }

    return empty_row


def check_for_failures(response, case_number):
    fail_state = False

    empty_row = return_empty_row(case_number)

    if response.status_code != 200:
        logger.info(f"Failed request: {response.status_code} status code")
        fail_state = True

    soup = BeautifulSoup(response.content, "html.parser")

    if soup.find(id="no-results"):
        logger.info("No results")
        fail_state = True

    if soup.find("div", class_="validation-summary-errors box-error"):
        error_message = soup.find(
            "div", class_="validation-summary-errors box-error"
        ).text
        logger.info(f"Error: {error_message}")
        fail_state = True

    return fail_state, empty_row


def scrape_results_for_case_number(
    url: str=None, party_id: str = None, case_number: str = None
):

    empty_row = return_empty_row(case_number)

    with requests.Session() as s:
        # search registryi depending on what ID is given
        if case_number:
            data = {"SearchMode": "CaseNumber", "CaseNumber": f"{case_number}"}

            r = s.post(url, data=data)

            # logger.info(f'Response status: {r.status_code}')
            fail_state, empty_row = check_for_failures(r, case_number)
            if fail_state:
                return empty_row

            soup = BeautifulSoup(r.content, "html.parser")

            # print(r.text)
            # If multiple results are returned:
            if soup.find("h1", text=re.compile("Search Results")):
                new_url = soup.find("td", class_="view").next_element.attrs["href"]

                r = s.get(
                    f"https://public.courts.in.gov{new_url}"
                )  # We choose the first

        elif party_id:
            r = s.get(
                f"https://public.courts.in.gov/grp/Search/Detail/{party_id}"
            ) 

        fail_state, empty_row = check_for_failures(r, case_number)
        if fail_state:
            return empty_row
            
        soup = BeautifulSoup(r.content, "html.parser")

        result_content = soup.find(
            id="form-con"
        )  # select the tag containing the case info

        scraped_case_number = re.findall("(?<=Case No\.\r\n)\s+[\d\w\-]+", result_content.text)[0].strip()

        print("SCRAPED", repr(scraped_case_number))

        p_tags = result_content.find_all(
            "p"
        )  # get all the 'p' tags, which contain the description info
        p_texts = [x.text for x in p_tags]  # get the text from all the p tags

        court_info_text = " ".join(soup.find("h6").text.split()).strip()
        court_info = court_info_text.split("In the")[1].split("Case No.")[0].strip()

        ward_name = soup.find("h2", class_="name").text.strip()

        ward_type = p_texts[0].strip()

        year_of_birth_tag = soup.find("p", text=re.compile("Year of Birth"))
        year_of_birth = year_of_birth_tag.text.strip()[-4:]

        guardianship_type_text = " ".join(
            soup.find("p", text=re.compile("Guardianship Type")).text.split()
        ).strip()
        guardianship_type = guardianship_type_text.split(":")[1].strip()

        guardian_names, guardianship_scope, issue_date, expiration_date = "", "", "", ""
        data_table_rows = soup.find_all("tr")
        rows_counter = 0
        for data_table_row in data_table_rows:
            if data_table_row.find("td"):
                if rows_counter > 0:
                    guardian_names += "; "
                    guardianship_scope += "; "
                    issue_date += "; "
                    expiration_date += "; "
                rows_counter += 1

                cells = data_table_row.find_all("td")

                guardian_names += cells[0].text.strip()
                guardianship_scope += cells[1].text.strip()
                issue_date += cells[2].text.strip()
                expiration_date += cells[3].text.strip()

        row = {
            "case_number": case_number or scraped_case_number,
            "court": court_info,
            "ward_name": ward_name,
            "ward_type": ward_type,
            "birth_year": year_of_birth,
            "guardianship_type": guardianship_type,
            "guardian(s)": guardian_names,
            "guardianship_scope": guardianship_scope,
            "issue_date": issue_date,
            "expiration_date": expiration_date,
        }

        return row


# if __name__ == "__main__":

#     url = "https://public.courts.in.gov/grp/"

#     results = []

#     casenumbers = pd.read_csv("projects/hhs/data/indiana/registry_party_ids_to_scrape.csv")

#     counter = 0

#     previous_time = datetime.now()

#     number_of_rows = casenumbers.shape[0]

#     for index, row in casenumbers.iterrows():
#         party_id = row["registry_id"]
#         logger.info(f"Registry Party ID: {party_id}")

#         row = scrape_results_for_case_number(party_id=party_id)
#         results.append(row)

#         counter += 1
#         if counter % 100 == 0:
#             last_hundred_rows_time = datetime.now() - previous_time
#             previous_time = datetime.now()

#             remaining_rows = number_of_rows - counter

#             estimated_remaining_time = last_hundred_rows_time * remaining_rows / 100

#             logger.info(f"Remaining case numbers: {remaining_rows}")
#             logger.info(
#                 f"Current pace: {last_hundred_rows_time.seconds} seconds per 100 rows"
#             )
#             logger.info(
#                 f"Estimated remaining time: {estimated_remaining_time.seconds / 3600} hours"
#             )

#     results_df = pd.DataFrame.from_dict(results)
#     results_filename = f"party_id_registry_scrape.csv"
#     results_df.to_csv(results_filename, index=False)
# exit()

if __name__ == "__main__":

    url = "https://public.courts.in.gov/grp/"

    results = []

    casenumbers = pd.read_csv("projects/hhs/analysis/case_numbers_to_scrape_registry.csv")

    counter = 0

    previous_time = datetime.now()

    number_of_rows = casenumbers.shape[0]

    for index, row in casenumbers.iterrows():
        case_number = row["case_number"]
        logger.info(f"Case number: {case_number}")

        row = scrape_results_for_case_number(case_number=case_number, url=url)
        results.append(row)

        counter += 1
        if counter % 100 == 0:
            last_hundred_rows_time = datetime.now() - previous_time
            previous_time = datetime.now()

            remaining_rows = number_of_rows - counter

            estimated_remaining_time = last_hundred_rows_time * remaining_rows / 100

            logger.info(f"Remaining case numbers: {remaining_rows}")
            logger.info(
                f"Current pace: {last_hundred_rows_time.seconds} seconds per 100 rows"
            )
            logger.info(
                f"Estimated remaining time: {estimated_remaining_time.seconds / 3600} hours"
            )

    results_df = pd.DataFrame.from_dict(results)
    results_filename = f"results_rescrape_for_missing_cases_6-4-2024.csv"
    results_df.to_csv(results_filename, index=False)
