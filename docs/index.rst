==============
 sqlite-utils
==============

*Python utility functions for manipulating SQLite databases*

This library aims to make creating a SQLite database from a collection of data as easy as possible.

It is not intended to be a full ORM: the focus is utility helpers to make creating the initial database and populating it with data as productive as possible.

In lieu of detailed documentation (coming soon), enjoy an example instead:

.. code-block:: python

    from sqlite_utils import db
    import sqlite3
    import requests
    import hashlib
    import json

    raw_ads = requests.get(
        "https://raw.githubusercontent.com/edsu/irads/master/ads.json"
    ).json()
    print(raw_ads[0])
    # {'clicks': 32,
    #  'created': '2016-11-14T04:10:27-08:00',
    #  'ended': None,
    #  'file': 'data/2016-11/P(1)0001720.pdf',
    #  'id': 3186,
    #  'impressions': 396,
    #  'spend': {'amount': '1050.77', 'currency': 'RUB'},
    #  'targeting': {'age': ['18 - 65+'],
    #   'excluded_connections': ['Exclude people who like Black guns matter'],
    #   'language': ['English (UK)', 'English (US)'],
    #   'location': ['United States'],
    #   'people_who_match': {'interests': ['Martin Luther King',
    #     'Jr.',
    #     '2nd Amendment',
    #     'National Rifle Association',
    #     'African-American culture',
    #     'African-American Civil Rights Movement (1954—68)',
    #     'Gun Owners of America',
    #     'African—American history',
    #     'Second Amendment to the United States Constitution',
    #     'Concealed carry in the United States',
    #     'Firearm',
    #     'Malcolm X']},
    #   'placements': ['News Feed on desktop computers',
    #    'News Feed on mobile devices',
    #    'Right column on desktop computers']},
    #  'text': 'Black American racial experience is real. We support the 2nd ammendment\nfor our safety.\n\n',
    #  'url': 'https://www.facebook.com/ProtectBIackGunOwners/'}

    def flatten_targeting(targeting, prefix=''):
        # Convert targeting nested dictionary into list of strings
        # e.g. people_who_match:interests:Martin Luther King
        if isinstance(targeting, list) and all(isinstance(s, str) for s in targeting):
            return ["{}:{}".format(prefix, item) for item in targeting]
        elif isinstance(targeting, str):
            return ["{}:{}".format(prefix, targeting)]
        elif isinstance(targeting, dict):
            items = []
            for key, value in targeting.items():
                new_prefix = "{}:{}".format(prefix, key) if prefix else key
                items.extend(flatten_targeting(value, new_prefix))
            return items

    def hash_id(s):
        return hashlib.md5(s.encode("utf8")).hexdigest()[:5]

    database = db.Database(sqlite3.connect("/tmp/ads3.db"))

    ads = database["ads"]
    targets = database["targets"]
    ad_targets = database["ad_targets"]

    for ad in raw_ads:
        record = {
            "id": ad["id"],
            "file": ad["file"],
            "clicks": ad["clicks"],
            "impressions": ad["impressions"],
            "text": ad["text"],
            "url": (ad["url"] or "").replace("httpszll", "https://"),
            "spend_amount": ad["spend"]["amount"],
            "spend_currency": ad["spend"]["currency"] or "USD",
            "created": ad["created"],
            "ended": ad["ended"],
        }
        ads.upsert(record, pk="id")
        for target in flatten_targeting(ad["targeting"]):
            target_id = hash_id(target)
            targets.upsert({
                "id": target_id,
                "name": target,
                "category": target.split(":")[0],
                "prefix": target.rsplit(":", 1)[0]},
                pk="id"
            )
            ad_targets.insert({
                "target_id": target_id,
                "ad_id": ad["id"],
            }, foreign_keys=(
                ("ad_id", "INTEGER", "ads", "id"),
                ("target_id", "TEXT", "targets", "id"),
            ))
