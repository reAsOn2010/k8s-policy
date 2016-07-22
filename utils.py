import json
import logging

from constants import *

from pycalico.datastore import TIER_PATH
from pycalico.datastore_datatypes import Rules, Rule

_log = logging.getLogger("__main__")


def find_policy_and_apply_to_profile(client, namespace_name):

    POLICIES_PATH = TIER_PATH + "/policy"

    profile_name = NS_PROFILE_FMT % namespace_name

    order_collection = PolicyCollection()
    profile = client.get_profile(profile_name)
    search_path = POLICIES_PATH % {"tier_name": NET_POL_TIER_NAME}
    for i in client.etcd_client.read(search_path).children:
        policy = json.loads(i.value)
        selector = "%s == '%s'" % (K8S_NAMESPACE_LABEL, namespace_name)
        if policy["selector"] == selector:
            order_collection.add(policy)
    inbound_rules = [rule for policy["inbound_rules"] in order_collection.get() for rule in policy["inbound_rules"]]
    outbound_rules = [rule for policy["outbound_rules"] in order_collection.get() for rule in policy["outbound_rules"]]
    profile.rules = Rules(id=profile.name,
                          inbound_rules=inbound_rules,
                          outbound_rules=outbound_rules)
    client.profile_update_rules(profile)


class PolicyCollection(object):

    def __init__(self):
        self._data = []

    def add(self, policy):
        if len(self._data) == 0:
            self._data.append(policy)
        elif self._data[0]["order"] < policy["order"]:
            pass
        elif self._data[0]["order"] == policy["order"]:
            policy["inbound_rules"] = [Rule(**rule) for rule in policy["inbound_rules"]]
            policy["outbound_rules"] = [Rule(**rule) for rule in policy["outbound_rules"]]
            self._data.append(policy)
        else:
            policy["inbound_rules"] = [Rule(**rule) for rule in policy["inbound_rules"]]
            policy["outbound_rules"] = [Rule(**rule) for rule in policy["outbound_rules"]]
            self._data = [policy]

    def get(self):
        return self._data
