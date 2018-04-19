from .transformation_utilities import *
import logging


"""A list of all possible status of the projects. A status which is not covered will return false."""
IMPORT_STATUS = {
    'Published': True,
    'Incomplete': False
}

"""The achievement types which should be imported. Other achievement types will be ignored."""
ACHIEVEMENT_TYPES = {
    "Mobility: Host of visiting scientists at Uni Basel": True,
    "Mobility: Visiting scientists at ...": True,
    "Cooperations": True,
    "Services: Organisation of scientific meetings": True,
    "Services to the public": True,
    "Invited presentations at conferences": True,
    "Awards, honours and prizes": True,
    "Memberships": True
}

"""These fields have no transformation function and are ignored by the transformation process. As
result these fields are not logged if there was a problem with the transformation (since there is always a problem)
These are logged on logging level INFO."""
IGNORE_LIST = {'rdborgid', 'lastupdate', 'status', 'author_mcssid', 'author_unibasCHpublicId', 'author_orcid'}

BASE_XML_PATH_ACHIEVEMENTS = './metadata/forschdb_achievement/'

if __name__ == '__main__':
    logging.basicConfig(filename='achievement_transformation.log', filemode='w', level=logging.WARNING)

    tf = TransformFDBRecord('ach', data_base_path='',
                            base_xml_path=BASE_XML_PATH_ACHIEVEMENTS,
                            import_filter={'status': IMPORT_STATUS, 'type': ACHIEVEMENT_TYPES},
                            ignore_list=IGNORE_LIST,
                            target_path='achievements/')

    """A transformations function for each field. Fields not defined here are ignored and logged."""
    TRANSFORMATION_FUNCTIONS_ACHIEVEMENTS = {
        'identifier': [tf.transform_to_list, {'edoc_tag': 'mcss_id'}],
        'mcssorgid': [tf.transform_to_list, {'edoc_tag': 'divisions'}],
        # TODO: transform achievement types.
        # Add only achievements which can be created. Nicolas will ask Hägele if any of the rest are needed.
        'type': [tf.transform_to_field, {'edoc_tag': 'achievement_type'}],

        # TODO: Really all title fields into title? Serves many different purposes per type.
        # Nicolas will ask easyWeb how they will implement this.
        'title': [tf.transform_to_field, {'edoc_tag': 'title'}],
        'achHonored': [tf.transform_to_field, {'edoc_tag': 'achHonored'}],

        # Since author information coming from RDF is not complete and the potential for several
        # authors in a single achievement only the 'author_dni' field is used to then enrich this data from
        # edoc dataservice.
        # If the DNI is not present in edoc dataservice the person index of the RDB is searched (indexed in elastic
        # by open access team, not actually harvested from the RDB since that would be very slow).
        # TODO: DNI may be NONE sometimes -> figure out what to do?
        'author_dni': [tf.transform_dni_to_contributor,
                                              {'edoc_tag': 'dni',
                                               'index': 'edoc-vmware',
                                               'doc_type': 'document',
                                               'url': config['elastic']['edoc_url'],
                                               'fdb_index': 'fdb-persons',
                                               'fdb_doc_type': 'publication',
                                               'fdb_url': config['elastic']['fdb_url']}
                       ],
        # These are ignored as they cannot be assigned if several DNIs are present in a single achievement.
        # 'author_orcid': [tf.add_to_contributor, {'edoc_tag': 'orcid'}],
        # 'author_unibasCHpublicId': [tf.add_to_contributor, {'edoc_tag': 'unibasChPublicId'}],
        'conference': [tf.transform_to_field, {'edoc_tag': 'event_title'}],
        'functionID': [tf.transform_to_field, {'edoc_tag': 'functionId'}],
        'functionText': [tf.transform_to_field, {'edoc_tag': 'functionText'}],

        # actually written like this in RDB...
        'fundingsorce': [tf.transform_to_list, {'edoc_tag': 'financed_by'}],

        'hoursAdmin': [tf.transform_to_field, {'edoc_tag': 'hoursAdmin'}],
        'hoursPresent': [tf.transform_to_field, {'edoc_tag': 'hoursPresent'}],
        'hoursRelation': [tf.transform_to_field, {'edoc_tag': 'hoursRelation'}],
        'hoursVoluntary': [tf.transform_to_field, {'edoc_tag': 'hoursVoluntary'}],
        'institution': [tf.transform_to_field, {'edoc_tag': 'institution'}],
        'invitedpers': [tf.transform_to_field, {'edoc_tag': 'invited_person'}],
        'numManuscripts': [tf.transform_to_field, {'edoc_tag': 'numManuscripts'}],
        'numProposals': [tf.transform_to_field, {'edoc_tag': 'numProposals'}],
        'placeLocation': [tf.transform_to_field, {'edoc_tag': 'event_location'}],
        'relatedProject': [tf.transform_to_field, {'edoc_tag': 'related_project'}],
        'scaleofcooperation': [tf.transform_to_field, {'edoc_tag': 'scale_of_cooperation'}],
        'typeofcooperation': [tf.transform_to_field, {'edoc_tag': 'type_of_cooperation'}],
        'typeofpresentation': [tf.transform_to_field, {'edoc_tag': 'type_of_presentation'}],
        'typeofservice': [tf.transform_to_field, {'edoc_tag': 'type_of_service'}],
        'startdate': [tf.transform_start_date, {'edoc_tag_start_date': 'date_start', 'edoc_tag_simple_date': 'date'}],
        'enddate': [tf.transform_to_field, {'edoc_tag': 'date_end'}],
    }

    tf.functions = TRANSFORMATION_FUNCTIONS_ACHIEVEMENTS
    tf.transform_all('achievements', 'achievement')













