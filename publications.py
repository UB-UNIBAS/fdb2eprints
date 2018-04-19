from transformation_utilities import *
import logging

"""A list of all possible status of the projects. Any status not covered will be assumed as False."""
IMPORT_STATUS = {
    'Published': True,
    'Verified': False,
    'Verified-plus': False,
    'Incomplete': False,
    'Erased': False,
    'Under Revision': False,
    'Complete - unpublished': False
}

PUBLICATION_TYPE = {
    'Publication: JournalArticle (Originalarbeit in einer wissenschaftlichen Zeitschrift)': True,
    'JournalItem (Kommentare, Editorials, Rezensionen, Urteilsanmerk., etc. in einer wissnsch. Zeitschr.': True,
    'Publication: Book Item (Buchkap., Lexikonartikel, jur. Kommentierung, Beiträge in Sammelbänden etc.': True,
    'Publication: ConferencePaper (Artikel, die in Tagungsbänder erschienen sind)': True,
    'Publication: Edited Book (Herausgeber eines eigenständigen Buches)': True,
    'Publication: Authored Book (Verfasser eines eigenständigen Buches)': True,
    'Publication: NewsItemPrint (Artikel in einer Tages, Wochen- oder Monatszeitschrift)': True,
    'Publication: Other Publications (Forschungsberichte o.ä.)': True,
    'Publication: Discussion paper / Internet publication': True,
    'Publication: NewsItemEmission (Radio - Fernsehbeiträge)': True,
    'Publication: Thesis (Dissertationen, Habilitationen)': False,
}

"""These fields have no transformation function and are ignored by the transformation process."""
IGNORE_LIST = set()

BASE_XML_PATH = './metadata/forschdb_publication/'

if __name__ == '__main__':
    logging.basicConfig(filename='publications_transformation.log', filemode='w', level=logging.DEBUG)

    tf = TransformFDBRecord('pub', data_base_path='',
                            base_xml_path=BASE_XML_PATH,
                            import_filter={'status': IMPORT_STATUS, 'type': PUBLICATION_TYPE},
                            ignore_list=IGNORE_LIST,
                            target_path='publications/')

    """A transformations function for each field. Fields not defined here are ignored and logged."""
    TRANSFORMATION_FUNCTIONS_PROJECTS = {
        'title': [tf.transform_publication_title, {'edoc_tag': 'title'}],


        'creator': [tf.transform_creators, {}],
        'pages': [tf.transform_publication_title, {'edoc_tag': 'pagerange'}],


        'easyWeb_appreance': [tf.transform_web_appearance, {'edoc_tag': 'hide_on_weblist'}],
    }

    tf.functions = TRANSFORMATION_FUNCTIONS_PROJECTS

    tf.transform_all('eprints', 'eprint')
