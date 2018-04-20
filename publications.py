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
IGNORE_LIST = {
                # handled by subtype: pubtype_weboffice.
                'type',
                # duplicate of mcssorgid
                'oaiorgid', 'rdborgid',
                # unused dc fields.
                'creator', 'editor', 'contributor',
                # is handled with together with date.
                'month_day',
                # duplicate of issn_isbn
                'isbn', 'issn_e', 'issn', 'isbn_e',
                'issue',  # duplicate of number
                # duplicate of journal
                'newspaper_title', 'publication',
                # duplicate of url
                'related_url',
                # duplicate of pages
                'pagerange',
                # fields not used in edoc or defined by edoc.
                'status', 'date_comment', 'unibascreator_mcssid', 'unibaseditor_mcssid', 'unibasauthor_mcssid',
                'edoc_url', 'isi_doctype', 'lastupdate', 'startdate', 'full_text_status', 'weifghtfactor',
                'series_number', 'volume_number'
               }

BASE_XML_PATH = './metadata/forschdb_publication/'

if __name__ == '__main__':
    logging.basicConfig(filename='publications_transformation.log', filemode='w', level=logging.WARNING)

    tf = TransformFDBRecord('pub', data_base_path='',
                            base_xml_path=BASE_XML_PATH,
                            import_filter={'status': IMPORT_STATUS, 'type': PUBLICATION_TYPE},
                            ignore_list=IGNORE_LIST,
                            target_path='publications/')

    """A transformations function for each field. Fields not defined here are ignored and logged."""
    TRANSFORMATION_FUNCTIONS_PROJECTS = {
        'title': [tf.transform_publication_title, {'edoc_tag': 'title'}],
        # Note: This function creates the type & note fields as well.
        'pubtype_weboffice': [tf.transform_pubtype_weboffice, {'edoc_tag': '_subtype'}],

        'creator': [tf.transform_creators, {}],
        'pages': [tf.transform_page_range, {}],

        'publisher': [tf.transform_to_field, {'edoc_tag': 'publisher'}],
        'institution': [tf.transform_to_field, {'edoc_tag': 'publisher'}],
        'stationname': [tf.transform_to_field, {'edoc_tag': 'publisher'}],
        'identifier': [tf.transform_to_list, {'edoc_tag': 'mcss_id'}],
        'description': [tf.transform_html_text, {'edoc_tag': 'abstract'}],
        'number': [tf.transform_to_field, {'edoc_tag': 'number'}],
        'place_of_conference': [tf.transform_to_field, {'edoc_tag': 'event_location'}],
        'dateofconference': [tf.transform_to_field, {'edoc_tag': 'event_dates'}],
        'series_title': [tf.transform_to_field, {'edoc_tag': 'series'}],
        'keywords': [tf.transform_to_field, {'edoc_tag': 'keywords'}],
        'volume': [tf.transform_to_field, {'edoc_tag': 'volume'}],
        'note': [tf.transform_to_field, {'edoc_tag': 'suggestions'}],
        'pubmed_entrezdate': [tf.transform_to_field, {'edoc_tag': 'entry_date'}],
        'fulltext_url': [tf.log_fulltext_url, {}],


        'series': [tf.transform_to_field, {'edoc_tag': 'series'}],
        'booktitle': [tf.transform_to_field, {'edoc_tag': 'book_title'}],
        'journal': [tf.transform_to_field, {'edoc_tag': 'publication'}],

        'doi': [tf.transform_id_number, {'type_tag': 'doi'}],
        'isi_number': [tf.transform_id_number, {'type_tag': 'isi'}],
        'pubmedid': [tf.transform_id_number, {'type_tag': 'pmid'}],

        'edition': [tf.transform_edition, {'edoc_tag': 'edition'}],

        'place_of_publication': [tf.transform_to_field, {'edoc_tag': 'place_of_pub'}],
        'country': [tf.transform_to_field, {'edoc_tag': 'place_of_pub'}],


        'easyWeb_appearance': [tf.transform_web_appearance, {'edoc_tag': 'hide_on_weblist'}],

        'addpublicationtranslation': [tf.append_to_field, {'edoc_tag': 'note',
                                                           'prefix': 'Additional publication or translation in: ',
                                                           'separator': ' -- '}],
        'genre': [tf.append_to_field, {'edoc_tag': 'note', 'prefix': 'Genre: ', 'separator': ' -- '}],
        'runningtime': [tf.append_to_field, {'edoc_tag': 'note', 'prefix': 'Running time: ', 'separator': ' -- '}],

        'unibasauthor': [tf.transform_persons, {'edoc_tag': 'contributors', 'type': 'author'}],
        'unibaseditor': [tf.transform_persons, {'edoc_tag': 'contributors', 'type': 'editor'}],
        'unibascreator': [tf.transform_persons, {'edoc_tag': 'submitter'}],
        'date': [tf.transform_date, {'edoc_tag': 'date'}],
        'refereed': [tf.transform_with_dict, {'edoc_tag': 'refereed', 'transformation_values': {
            'Peer reviewed': 'TRUE',
            'Yes': 'TRUE',
            'Not peer reviewed': 'FALSE'}}],
        'unibasel_publication': [tf.transform_with_dict, {'edoc_tag': 'unibaspub', 'transformation_values': {'Yes': 'TRUE', 'No': 'FALSE'}}],
        'url': [tf.transform_related_url, {'edoc_tag': 'related_url', 'url_type': 'doc'}],
        'issn_isbn': [tf.transform_issn_isbn, {}],
        # Note: Adds the department as well.
        'mcssorgid': [tf.transform_mcssorgid, {'edoc_tag': 'divisions'}],
    }

    ADD_STATIC_FIELDS = {
        'date_type': 'published',
        'eprints_status': 'buffer',
        'has_mcss_id': '1',

    }

    tf.functions = TRANSFORMATION_FUNCTIONS_PROJECTS
    tf.static_fields = ADD_STATIC_FIELDS

    tf.transform_all('eprints', 'eprint')
