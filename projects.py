from transformation_utilities import *
import logging

"""A list of all possible status of the projects. Any status not covered will be assumed as False."""
IMPORT_STATUS = {
    'Published': True,
    'Archived': True,
    'Incomplete new': False,
    'Incomplete': False,
    'Archive (internal use only)': False,
    'Published (internal use only)': False,
    'Rejected': False,
    'Archived (hidden)': False,
    'Incomplete new (hidden)': False
}

"""These fields have no transformation function and are ignored by the transformation process."""
IGNORE_LIST = {'creator', 'date', 'principalinvestigator_dni', 'principalinvestigator_mcssid', 'projectmember_dni',
               'projectmember_mcssid', 'rdborgid', 'status', 'lastupdate', 'creationdate', 'oaiorgid',
               'coinvestigator_dni', 'coinvestigator_mcssid', 'unibascreator_dni', 'unibascreator_mcssid',
               'unibascreator_unibasCHpublicId', 'unibascreator_orcid'}

BASE_XML_PATH_PROJECTS = './metadata/forschdb_project/'

if __name__ == '__main__':
    logging.basicConfig(filename='projects_transformation.log', filemode='w', level=logging.WARNING)

    tf = TransformFDBRecord('proj', data_base_path='',
                            base_xml_path=BASE_XML_PATH_PROJECTS,
                            import_filter={'status': IMPORT_STATUS},
                            ignore_list=IGNORE_LIST,
                            target_path='projects/')

    """A transformations function for each field. Fields not defined here are ignored and logged."""
    TRANSFORMATION_FUNCTIONS_PROJECTS = {
        'identifier': [tf.transform_to_list, {'edoc_tag': 'mcss_id'}],
        'mcssorgid': [tf.transform_to_list, {'edoc_tag': 'divisions'}],
        'type': [tf.transform_project_type, {'edoc_tag': 'type'}],
        'title': [tf.transform_html_text, {'edoc_tag': 'title'}],
        'sapnumber': [tf.transform_to_field, {'edoc_tag': 'sap_number'}],
        'url': [tf.transform_to_field, {'edoc_tag': 'related_url'}],
        'startdate': [tf.transform_to_field, {'edoc_tag': 'date_start'}],
        'enddate': [tf.transform_to_field, {'edoc_tag': 'date_end'}],
        'coverage': [tf.transform_project_status, {'edoc_tag': 'project_status'}],
        'keywords': [tf.transform_to_field, {'edoc_tag': 'keywords'}],
        'description': [tf.transform_html_text, {'edoc_tag': 'abstract'}],
        'coinvestigator': [tf.transform_persons, {'edoc_tag': 'coinvestigator'}],
        'principalinvestigator': [tf.transform_persons, {'edoc_tag': 'investigator'}],
        'projectmember': [tf.transform_persons, {'edoc_tag': 'member'}],
        'unibascreator': [tf.transform_persons, {'edoc_tag': 'submitter'}],
        'financedby': [tf.transform_financed_by, {'edoc_tag': 'financed_by'}],
        'affilatedpublication': [tf.transform_affiliated_publication, {'edoc_tag': 'publications',
                                                                       'index': 'edoc-vmware',
                                                                       'doc_type': 'document',
                                                                       'url': config['elastic']['edoc_url']}],
        'cooperation': [tf.transform_to_list, {'edoc_tag': 'cooperation'}]
    }

    tf.functions = TRANSFORMATION_FUNCTIONS_PROJECTS

    tf.transform_all('projects', 'project')
