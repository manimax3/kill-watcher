# Functions return true if the kill should not be filtered
# Always return (bool, reason)


def corporations(killmail, filter_corporations, filter_if_victim):
    if len(filter_corporations) == 0:
        # No corporations to be filtered
        return True, ""

    # Remove duplicates
    filter_corporations = set(filter_corporations)

    try:
        defender_corp = killmail["victim"]["corporation_id"]
        # Returns the unique set of corporation_ids of the attacker which do have
        # a corporation_id
        attacker_corporations = set([
            k["corporation_id"] for k in filter(
                lambda k: "corporation_id" in k, killmail["attackers"])
        ])
    except KeyError:
        defender_corp = ""
        attacker_corporations = set()

    if filter_if_victim and defender_corp in filter_corporations:
        return False, "Defenders Corporation filtered"

    if len(attacker_corporations) > 0 and len(
            filter_corporations.intersection(attacker_corporations)) > 0:
        return False, "Attackers Corporation filtered"

    return True, ""


def ship_type(killmail, filter_ship_types):

    ship_type = killmail["victim"]["ship_type_id"]

    if ship_type in filter_ship_types:
        print("filtered?")
        return False, "Ship type filtered"

    print("not filtered?")
    return True, ""


def highsec(system):
    if system["security_status"] >= 0.5:
        return False, "Happend in hs"

    return True, ""
