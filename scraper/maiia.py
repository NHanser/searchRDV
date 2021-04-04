import json
import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup


DEBUG = True
BASE_AVAILIBILITY_URL = "https://www.maiia.com/api/pat-public/availability-closests"

session = requests.Session()


def fetch_slots(rdv_site_web):
    response = session.get(rdv_site_web)
    soup = BeautifulSoup(response.text, 'html.parser')
    response.raise_for_status()

    rdv_form = soup.find(id="__NEXT_DATA__")
    if rdv_form:
        return get_slots_from(rdv_form, rdv_site_web)

    return None


def get_slots_from(rdv_form, rdv_url):
    json_form = json.loads(rdv_form.contents[0])

    rdv_form_attributes = ['props', 'initialState', 'cards', 'item', 'center']
    tmp = json_form

    # Étant donné que l'arbre des attributs est assez cossu / profond, il est préférable
    # d'itérer et de vérifier à chaque fois que les attributs recherchés sont bien
    # présents dans l'arbre afin de ne pas avoir d'erreurs inattendues.
    for attr in rdv_form_attributes:
        if tmp is not None and attr in tmp:
            tmp = tmp[attr]
        else:
            return None

    center_infos = tmp
    center_id = center_infos['id']

    availability = get_any_availibility_from(center_id)
    if availability["availabilityCount"] == 0:
        return None

    if "firstPhysicalStartDateTime" in availability:
        dt = datetime.strptime(availability['firstPhysicalStartDateTime'],
                               '%Y-%m-%dT%H:%M:%S.%fZ')
        dt = dt + timedelta(hours=2)
        dt = dt.strftime("%Y-%m-%d %H:%M")
        return dt

    # Ne sachant pas si 'firstPhysicalStartDateTime' est un attribut par défault dans
    # la réponse, je préfère faire des tests sur l'existence des attributs
    if "closestPhysicalAvailability" in availability and "startDateTime" in availability:
        dt = datetime.strptime(
                availability['closestPhysicalAvailability']["startDateTime"],
                '%Y-%m-%dT%H:%M:%S.%fZ')
        dt = dt + timedelta(hours=2)
        dt = dt.strftime("%Y-%m-%d %H:%M")
        return dt


    return None


def get_any_availibility_from(center_id):
    request_params = {
        "date_str": datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
        "centerId": center_id,
        "limit": 200,
        "page": 0,
    }

    availability = session.get(BASE_AVAILIBILITY_URL, params=request_params)
    return availability.json()
