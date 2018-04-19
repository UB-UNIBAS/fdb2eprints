from simple_elastic import ElasticIndex
from rdb_harvest import HarvestFDBData

from xml.etree import ElementTree as ET
from datetime import datetime
from itertools import zip_longest
import logging
import collections
import html
import os
import re

from configparser import ConfigParser


config = ConfigParser()
config.read('default.cfg')


class TransformFDBRecord(collections.Sequence):

    def __init__(self, record_type: str,
                 data_base_path: str,
                 base_xml_path: str,
                 import_filter: dict,
                 ignore_list: set,
                 target_path: str,
                 logger=logging.getLogger(__name__.split('.')[-1])):

        self.record_type = record_type
        self.data_path = data_base_path
        self.base_xml_path = base_xml_path
        self.import_filter = import_filter
        self.target_path = target_path
        self.functions = None
        self.ignore_list = ignore_list
        self.logger = logger
        self.current_id = ''
        self.current_title = ''
        self.current_type = ''
        self.current_subtype = ''


        self.publication_types = {
            'Publication: JournalArticle (Originalarbeit in einer wissenschaftlichen Zeitschrift)': 'article',
            'JournalItem (Kommentare, Editorials, Rezensionen, Urteilsanmerk., etc. in einer wissnsch. Zeitschr.': 'article',
            'Publication: Book Item (Buchkap., Lexikonartikel, jur. Kommentierung, Beiträge in Sammelbänden etc.': 'book_section',
            'Publication: ConferencePaper (Artikel, die in Tagungsbänder erschienen sind)': 'conference_item',
            'Publication: Edited Book (Herausgeber eines eigenständigen Buches)': 'book',
            'Publication: Authored Book (Verfasser eines eigenständigen Buches)': 'book',
            'Publication: NewsItemPrint (Artikel in einer Tages, Wochen- oder Monatszeitschrift)': 'contribution_to_periodical',
            'Publication: Other Publications (Forschungsberichte o.ä.)': 'other',
            'Publication: Discussion paper / Internet publication': 'working_paper',
            'Publication: NewsItemEmission (Radio - Fernsehbeiträge)': 'audio_visual',
            'Publication: Thesis (Dissertationen, Habilitationen)': 'thesis'
        }

    def harvest(self, use_last_update=False):
        harvester = HarvestFDBData(user=config['fdb-harvest']['user'],
                                   password=config['fdb-harvest']['password'],
                                   base_path=self.data_path)
        harvester.harvest(self.record_type, date=self.last_update if use_last_update else None)

    @property
    def last_update(self):
        last_update = None
        with open('last_update.txt', 'r') as file:
            t = file.read()
            if t != '':
                last_update = datetime.strptime(t, '%d-%m-%Y %H:%M:%S')
            if last_update is None:
                last_update = datetime.today()

        with open('last_update.txt', 'w') as file:
            file.write(datetime.today().strftime('%d-%m-%Y %H:%M:%S'))
        return last_update

    @property
    def path(self):
        return self.data_path + config['data'][self.record_type]

    @property
    def file_names(self):
        return [root + file for root, dirs, files in os.walk(self.path) for file in files]

    def __len__(self):
        root, directories, files = os.walk(self.path)
        return len(files)

    def __getitem__(self, item):
        logging.debug('Remove all namespace tags from XML elements for better processing.')
        # uses the encoding specified inside of the xml.
        tree = ET.iterparse(self.file_names[item])
        # Remove namespaces as they are not properly supported in xmljson and would clutter the field names in ES.
        for _, element in tree:
            try:
                element.tag = element.tag.split('}', 1)[1]
            except IndexError:
                pass
        logging.debug('Successfully removed all namespace tags from XML elements.')
        return tree.root.find('./ListRecords')

    def filter_record(self, record):
        add = False
        for filter in self.import_filter:
            field = record.find(self.base_xml_path + filter)
            if field is not None:
                try:
                    add = self.import_filter[filter][field.text]
                except KeyError:
                    add = False
        return add

    def transform_record(self, parent, record, record_name):
        if record.tag == 'record':
            if self.filter_record(record):
                c = ET.SubElement(parent, record_name, xmlns='http://eprints.org/ep2/data/2.0')
                fields = list(record.findall(self.base_xml_path))
                for element in fields:
                    if element.tag == 'title':
                        self.current_title = element.text
                    elif element.tag == 'identifier':
                        self.current_id = element.text
                    elif element.tag == 'type':
                        self.current_type = element.text
                    elif element.tag == 'pubtype_weboffice':
                        self.current_subtype = element.text
                for element in fields:
                    if element.tag in self.functions:
                        self.functions[element.tag][0](element, c, **self.functions[element.tag][1])
                    else:
                        if element.tag in self.ignore_list:
                            self.logger.info('This element could not be transformed: %s.', element.tag)
                        else:
                            self.logger.error('This element could not be transformed: %s.', element.tag)

    def transform_all(self, area_name, record_name, size=1000):
        def chunk(iterable, n, fillvalue=None):
            args = [iter(iterable)] * n
            return zip_longest(*args, fillvalue=fillvalue)

        x = 0
        for item in self:
            for record in chunk(item, size):
                eprints = ET.Element(area_name)
                for r in record:
                    if r is not None:
                        self.transform_record(eprints, r, record_name=record_name)

                with open(self.target_path + '{}-{}.xml'.format(self.record_type, size + x), 'w', encoding='utf-8') as file:
                    file.write(ET.tostring(eprints, encoding='utf-8').decode('utf-8'))
                x += 1

    def transform_to_list(self, element, parent, edoc_tag):
        """Searches for element with edoc_tag as tag in parent. If not found creates the element. Adds a item to the
        new element."""
        item = parent.find('./' + edoc_tag)
        if item is None:
            item = ET.SubElement(parent, edoc_tag)
        ET.SubElement(item, 'item').text = element.text.strip()

    def transform_to_field(self, element, parent, edoc_tag):
        """Simply translates the field from RDB to edoc."""
        ET.SubElement(parent, edoc_tag).text = element.text.strip()
        if edoc_tag == 'title':
            with open('titles.txt', 'a') as file:
                file.write(element.text + '\n')

    def transform_name(self, element, parent):
        """Transform a name element. Parent: name-XML-element."""
        if element.tag.endswith('firstname'):
            self.transform_to_field(element, parent, 'given')
        elif element.tag.endswith('lastname'):
            self.transform_to_field(element, parent, 'family')
        elif element.tag.endswith('initials'):
            parent.find('./given').text += ' ' + element.text.strip()
        else:
            if element.tag not in self.ignore_list:
                logging.error('Ignoring the following field in person element: %s', element.tag)
            else:
                logging.debug('Ignoring the following field in person element: %s', element.tag)

    def transform_persons(self, element, parent, edoc_tag):
        """Transform a unibas contributor."""
        person = parent.find('./' + edoc_tag)
        if person is None:
            person = ET.SubElement(parent, edoc_tag)
        eprint_item = ET.SubElement(person, 'item')
        name_item = ET.SubElement(eprint_item, 'name')
        for item in element:
            if item.tag.endswith('dni'):
                self.transform_to_field(item, eprint_item, 'dni')
            elif item.tag.endswith('email'):
                self.transform_to_field(item, eprint_item, 'id')
            elif item.tag.endswith('orcid'):
                self.transform_to_field(item, eprint_item, 'orcid')
            elif item.tag.endswith('unibasCHpublicId'):
                self.transform_to_field(item, eprint_item, 'unibasChPublicId')
            else:
                self.transform_name(item, name_item)

    def transform_submitters(self, element, parent, edoc_tag):
        """Same as person, but ignored unibasCHpublicId, DNI and ORCID."""
        person = parent.find('./' + edoc_tag)
        if person is None:
            person = ET.SubElement(parent, edoc_tag)
        eprint_item = ET.SubElement(person, 'item')
        name_item = ET.SubElement(eprint_item, 'name')
        for item in element:
            tag = item.tag
            if tag.endswith('email'):
                self.transform_to_field(item, eprint_item, 'id')
            elif tag.endswith('name') or tag.endswith('initials'):
                self.transform_name(item, name_item)
            else:
                if item.tag not in self.ignore_list:
                    logging.error('Ignoring field %s for submitters.', item.tag)
                else:
                    logging.debug('Ignoring field %s for submitters.', item.tag)

    def transform_html_text(self, element, parent, edoc_tag=''):
        """Transform abstract with special processing from projects."""
        text = element.text
        text = html.unescape(text)                  # replace html escaped characters with plain text
        text = re.sub('<!--.*?-->', '', text)       # remove xml comments (Doc Style Documentation)
        text = re.sub('<br\\ >', ' ', text)         # replace <br\ > tags with spaces
        text = re.sub('<[^<]+?>', '', text)         # remove html tags
        text = re.sub(u'\\x84', '"', text)          # replace double low quotation mark
        text = re.sub(u'\\xAD', '', text)           # remove soft hyphen
        text = re.sub(u'\\x96', '-', text)          # replace en-Dash
        text = re.sub(u'\\x97', '-', text)          # replace em-Dash
        text = re.sub(u'\\x93', '"', text)          # replace left double quotation mark
        text = re.sub(u'\\x94', '"', text)          # replace right double quotation mark
        text = re.sub(u'\\x95', '- ', text)         # replace bullet points
        text = re.sub(u'\\x91', "'", text)          # replace left single quotation mark
        text = re.sub(u'\\x92', "'", text)          # replace right single quotation mark
        text = re.sub(u'\\x0A', '', text)           # remove non-breaking spaces.

        text = re.sub(u'\\u00AC', '', text)         # remove not sign.
        text = re.sub('\s+', ' ', text)             # replace any collection of whitespace characters with a single space.
        text = re.sub('Normal .*?bidi;\}', '', text)  # HACK: special stuff that for some reason isn't removed with the HTML Tags.
        text = text.strip()                         # remove trailing & leading white space
        if text != '':
            ET.SubElement(parent, edoc_tag).text = text
        else:
            logging.info('Abstract has been removed from element as it was an empty string after transformation!')

    def transform_affiliated_publication(self, element, parent, edoc_tag, index, doc_type, url):
        """Transform affiliated publications in projects.

        Uses the given elastic index to translate a mcss id into a eprints id.

        When a duplicate is found, all eprints Ids are added and the logging is sent to fodaba@unibas.ch.
        De-duplication has to be resolved manually.

        When no match is found the mcss id is ignored. (TODO: send to fodaba@unibas.ch?)
        """
        field = parent.find('./' + edoc_tag)
        if field is None:
            field = ET.SubElement(parent, edoc_tag)
        es = ElasticIndex(index, doc_type, url=url)
        query = {'_source': ['eprintid'], 'query': {'term': {'mcss_id': {'value': int(element.text)}}}}
        result = es.scan_index(query)
        if len(result) == 1:
            ET.SubElement(field, 'item').text = str(result[0]['eprintid'])
        elif len(result) > 1:
            for e in result:
                ET.SubElement(field, 'item').text = str(e['eprintid'])
            logging.error('Found multiple results with mcss_id %s for project %s %s.',
                          element.text, self.current_id, self.current_title)
        else:
            logging.error('Found no eprints ID for the following mcss_id: %s for project %s, %s.',
                          element.text, self.current_id, self.current_title)

    def transform_dni_to_contributor(self, element, parent, edoc_tag,
                                     index='', doc_type='', url='',
                                     fdb_index='', fdb_doc_type='', fdb_url=''):
        """Uses the given DNI to add a full contributor.

        With the given DNI first edoc dataservice will be searched for a match. If found all data from this
        contributor is copied over.

        If the edoc dataservice turns up empty the RDB Persons Database is searched. If found all data is copied over.
        """
        if element.text is not None:
            es = ElasticIndex(index, doc_type, url=url)
            query = {
                '_source': ['contributors'],
                'query': {'term': {'contributors.dni.keyword': {'value': int(element.text)}}}}
            results = es.scan_index(query)
            if len(results) > 0:
                # returns all contributors. Only add the one with the right DNI.
                for contrib in results[0]['contributors']:
                    if 'dni' in contrib and str(contrib['dni']) == element.text:
                        contributor = parent.find('./contributor')
                        if contributor is None:
                            contributor = ET.SubElement(parent, 'contributor')
                        item = ET.SubElement(contributor, 'item')
                        ET.SubElement(item, 'dni').text = str(contrib['dni']).strip()
                        name = ET.SubElement(item, 'name')
                        ET.SubElement(name, 'given').text = str(contrib['name']['given']).strip()
                        ET.SubElement(name, 'family').text = str(contrib['name']['family']).strip()
                        if 'id' in contrib:
                            ET.SubElement(item, 'id').text = str(contrib['id']).strip()
                        if 'orcid' in contrib:
                            ET.SubElement(item, 'orcid').text = str(contrib['orcid']).strip()
                        if 'unibasChPublicId' in contrib:
                            ET.SubElement(item, 'unibasChPublicId').text = str(contrib['unibasChPublicId']).strip()
            else:  # len(results) == 0
                # try to search it in RDB persons database.
                fdb = ElasticIndex(fdb_index, fdb_doc_type, url=fdb_url)
                query = {'query': {'term': {'dni.keyword': {'value': int(element.text)}}}}
                fdb_results = fdb.scan_index(query)
                if len(fdb_results) == 0:
                    self.logger.error('Could not find an author with dni %s.', element.text)
                elif len(fdb_results) == 1:
                    # in case of a single find => add the contributor to the list.
                    contributor = parent.find('./contributor')
                    if contributor is None:
                        contributor = ET.SubElement(parent, 'contributor')
                    item = ET.SubElement(contributor, 'item')
                    r = fdb_results[0]
                    ET.SubElement(item, 'id').text = r['email'].strip()
                    if 'unibasCHpublicId' in r:
                        ET.SubElement(item, 'unibasChPublicId').text = r['unibasCHpublicId']
                    if 'orcid' in r:
                        ET.SubElement(item, 'orcid').text = r['orcid'].strip()
                    ET.SubElement(item, 'dni').text = str(r['dni']).strip()
                    name = ET.SubElement(item, 'name')
                    ET.SubElement(name, 'given').text = r['firstname'].strip()
                    ET.SubElement(name, 'family').text = r['lastname'].strip()
                else:
                    # Should never happen...
                    self.logger.critical('Found several persons with DNI %s in RDB.', element.text)
        else:
            self.logger.error('A DNI in element %s is None.', self.current_id)

    def transform_start_date(self, element, parent, edoc_tag_start_date, edoc_tag_simple_date):
        """Transform achievement startdate.

        Depending on the type of the achievement something else will be done.

        Startdate:
            - "Mobility: Host of visiting scientists at Uni Basel"
            - "Mobility: Visiting scientists at ..."
            - "Cooperations"
            - "Services: Organisation of scientific meetings"
            - "Memberships"
        Date:
            - "Invited presentations at conferences"
            - "Awards, honours and prizes"
        """
        if self.current_type == 'Invited presentations at conferences' or self.current_type == 'Awards, honours and prizes':
            ET.SubElement(parent, edoc_tag_simple_date).text = element.text
        else:
            ET.SubElement(parent, edoc_tag_start_date).text = element.text

    def transform_project_type(self, element, parent, edoc_tag):
        """Transforms the type of projects to the edoc equivalent."""
        if element.text == 'Project: Third-party funded project':
            ET.SubElement(parent, edoc_tag).text = 'third_party'
        elif element.text == 'Project: Project funded by own resources':
            ET.SubElement(parent, edoc_tag).text = 'own_resource'
        else:
            self.logger.critical('Unknown project type: %s.', element.text)

    @staticmethod
    def transform_project_status(element, parent, edoc_tag):
        """Transforms the project status."""
        status = {'Completed': 'complete', 'Active': 'ongoing'}
        ET.SubElement(parent, edoc_tag).text = status[element.text]

    @staticmethod
    def transform_financed_by(element, parent, edoc_tag):
        """Transform project financed_by."""
        field = parent.find('./' + edoc_tag)
        if field is None:
            field = ET.SubElement(parent, edoc_tag)
        item = ET.SubElement(field, 'item')
        ET.SubElement(item, 'name').text = element.text.strip()

    def transform_web_appearance(self, element, parent, edoc_tag):
        """Add hide_on_weblist if easyWeb_appearance = "Do not show on easyWeb-Pages"."""
        if element.text == 'Do not show on easyWeb-Pages':
            logging.info('%s is hidden on web page.', self.current_id)
            ET.SubElement(parent, edoc_tag).text = 'TRUE'
        else:
            ET.SubElement(parent, edoc_tag).text = 'FALSE'

    def transform_publication_title(self, element, parent, edoc_tag):
        """Transforms the title of a publication.

        Removes special signs, html stuff & a dot at the end if this is a pub-med import."""
        if parent.find('./pubmedid'):
            element.text = element.text.strip('.')
        self.transform_html_text(element, parent, edoc_tag)

    def transform_page_range(self, element, parent, edoc_tag):
        """Transform publication page ranges.

        Removes common prefixes & whitespaces.
        Expands page range if second part is shortened."""
        pages = element.text.strip()
        pages = re.sub('^S\. ', '', pages)
        pages = re.sub('^p\. ', '', pages)
        if self.current_type in []
        page_range = element.text
        if page_range.count('-') == 1:
            f, s = page_range.split('-')
            try:
                first_number = int(f)
                second_number = int(s)
            except ValueError:
                pass
            else:
                new_second = ''
                if second_number < first_number:
                    for i in range(len(f) - len(s)):
                        new_second += str(f[i])
                    page_range = str(f) + '-' + new_second + str(s)

        ET.SubElement(parent, edoc_tag).text = page_range

    def transform_publication_type(self, element, parent, edoc_tag):
        """Transform the publication type.

        Creates the note as well.
        """
        self.create_note_from_type(element.text, parent)
        value = element.text

        if re.search('Authored Book|Edited Book', value):
            element.text = 'book'
        elif re.search('JournalArticle|JournalItem', value):
            element.text = 'article'
        elif re.search('Book Item', value):
            element.text = 'book section'
        elif re.search('ConferencePaper', value):
            element.text = 'conference_item'
        elif re.search('Thesis', value):
            element.text = 'thesis'
        # Note: Only discussion papers with the subtype internet publication become type preprint.
        elif re.search('Discussion paper', value) and self.current_subtype == 'Internet publication':
            element.text = 'preprint'
        elif re.search('Discussion paper', value):
            element.text = 'working_paper'
        elif re.search('NewsItemPrint', value):
            element.text = 'contribution_to_peridocal'
        elif re.search('NewsItemEmission', value):
            element.text = 'audio_visual'

        self.transform_to_field(element, parent, edoc_tag)

    def transform_pubtype_weboffice(self, element, parent, edoc_tag):
        """
        """

        subtypes = {
            'Review':'review',
            'Rezension': 'book_review',
            'Urteilsanmerkung': 'annotation',
            'Aufsatz/Beitrag in Sammelband': 'chapter',
            'Lexikonartikel': 'encyclopedia',
            'Jur. Kommentierung': 'commentary',
            'Übersetzung': 'contribution'
        }

        if element.text in subtypes:
            text = subtypes[element.text]
        elif element.text == 'Edition' and re.search('Editied Book', self.current_type):
            text = 'contribution'
        elif re.search('JournalArticle', self.current_type):
            text = 'research'
        elif re.search('JournalItem', self.current_type):
            text = 'contribution'
        elif re.search('Authored Book', self.current_type):
            text = 'authored'
        elif re.search('Edited Book', self.current_type):
            text = 'edited'
        elif self.publication_types[self.current_type] == 'book_section':
            text = 'contribution'
        elif re.search('ConferencePaper', self.current_type):
            text = 'paper'
        else:
            text = ''
            self.logger.error('Could not determine subtype of %s with type %s and subtype %s.', self.current_id,
                              self.current_type, self.current_subtype)

        ET.SubElement(parent, edoc_tag).text = text

    @staticmethod
    def create_note_from_type(value, parent):
        """Generate the note from the RDB type."""
        value = re.sub('Publication: ', 'Publication type according to Uni Basel Research Database: ', value)

        value = re.sub('Authored Book \(Verfasser eines eigenst.ndigen Buches\)', 'Authored book', value)
        value = re.sub('Book Item \(Buchkap\., Lexikonartikel, jur\. Kommentierung, Beiträge in Sammelbänden etc\.\)',
                       'Book item', value)
        value = re.sub('ConferencePaper \(Artikel, die in Tagungsb.nden erschienen sind\)', 'Conference paper', value)
        value = re.sub('Edited Book \(Herausgeber eines eigenst.ndigen Buches\)', 'Edited book', value)
        value = re.sub('JournalArticle \(Originalarbeit in einer wissenschaftlichen Zeitschrift\)', 'Journal article', value)

        # Note: 'JournalItem' lacks the leading 'Publication: ' and the closing parenthesis.
        value = re.sub('JournalItem \(Kommentare, Editorials, Rezensionen, Urteilsanmerk\., etc\. in einer wissensch\. Zeitschr\.',
                       'Publication type according to Uni Basel Research Database: Journal item', value)
        value = re.sub('NewsItemEmission \(Radio - Fernsehbeitr.ge\)', 'News item emission', value)
        value = re.sub('NewsItemPrint \(Artikel in einer Tages, Wochen- oder Monatszeitschrift\)', 'News item print', value)
        value = re.sub('Other Publications \(Forschungsberichte o\. .\.\)', 'Other publications', value)

        ET.SubElement(parent, 'note').text = value

    @staticmethod
    def transform_creators(element, parent):
        """Transform the creators list into given / family name pairs."""
        # TODO: Add clean up routines.
        creators = ET.SubElement(parent, 'creator')
        names = element.text.split(';')

        for name in names:
            family, given = name.split(',')
            item = ET.SubElement(creators, 'item')
            ET.SubElement(item, 'family').text = family.strip()
            ET.SubElement(item, 'given').text = given.strip()











