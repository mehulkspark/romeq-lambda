import io
import json
import logging
import os
from tempfile import NamedTemporaryFile
import time
from urllib.parse import urlparse
from uriutils import uri_read
from utils import get_subprocess_output

LAMBDA_TASK_ROOT = os.environ.get('LAMBDA_TASK_ROOT', os.path.dirname(os.path.abspath(__file__)))
LAMBDA_FUNCTION_NAME = os.environ['LAMBDA_FUNCTION_NAME']
BIN_DIR = os.path.join(LAMBDA_TASK_ROOT, 'bin')
LIB_DIR = os.path.join(LAMBDA_TASK_ROOT, 'lib')

MERGE_SEARCHABLE_PDF_DURATION = float(os.environ.get('MERGE_SEARCHABLE_PDF_DURATION', 90))
RETURN_RESULTS_DURATION = float(os.environ.get('RETURN_RESULTS_DURATION', 3.0))
TEXTRACT_OUTPUT_WAIT_BUFFER_TIME = float(os.environ.get('TEXTRACT_OUTPUT_WAIT_BUFFER_TIME', 5.0))


logging.basicConfig(format='%(asctime)-15s [%(name)s-%(process)d] %(levelname)s: %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


def handle(event, context):
    global logger

    document_uri = event['document_uri']
    page = event.get('page')

    start_time = time.time()

    logger.info('{} invoked with event {}.'.format(os.environ['AWS_LAMBDA_FUNCTION_NAME'], json.dumps(event)))

    o = urlparse(document_uri)
    _, ext = os.path.splitext(o.path)  # get format from extension
    ext = ext.lower()

    extract_func = PARSE_FUNCS.get(ext)
    if extract_func is None:
        raise ValueError('<{}> has unsupported extension "{}".'.format(document_uri, ext))

    with NamedTemporaryFile(mode='wb', suffix=ext, delete=False) as f:
        document_path = f.name
        f.write(uri_read(document_uri, mode='rb'))
        logger.debug('Downloaded <{}> to <{}>.'.format(document_uri, document_path))
    # end with

    try:
        textractor_results = extract_func(document_path, event, context)



    except Exception as e:
        logger.exception('Extraction exception for <{}>'.format(document_uri))
        return {'status': 'failed', "page": page, 'message': ""}
    finally:
        os.remove(document_path)
    # end try

    return {'status': 'success', "page": page, 'message': textractor_results["text"]}


# end def


def _pdf_to_text(document_path):
    text_path = document_path + '.txt'
    _get_subprocess_output(
        [os.path.join(BIN_DIR, 'pdftotext'), '-layout', '-nopgbrk', '-eol', 'unix', document_path, text_path],
        shell=False, env=dict(LD_LIBRARY_PATH=os.path.join(LIB_DIR, 'pdftotext')))

    with io.open(text_path, mode='r', encoding='utf-8', errors='ignore') as f:
        text = f.read().strip()
    os.remove(text_path)

    return text


# end def


def _get_subprocess_output(*args, **kwargs):
    global logger

    return get_subprocess_output(*args, logger=logger, **kwargs)


# end def


def pdf_to_text_with_ocr(document_path, event, context, create_searchable_pdf=True):
    global logger

    page = event.get('page')

    if page is not None:
        return pdf_to_text_with_ocr_single_page(document_path, event, context,
                                                create_searchable_pdf=create_searchable_pdf)


def pdf_to_text_with_ocr_single_page(document_path, event, context, create_searchable_pdf=True):
    page = event['page']

    with NamedTemporaryFile(suffix='.png', delete=False) as f:
        image_page_path = f.name

    try:
        cmdline = [os.path.join(BIN_DIR, 'gs'), '-sDEVICE=png16m', '-dFirstPage={}'.format(page+1),
                   '-dLastPage={}'.format(page+1), '-dINTERPOLATE', '-r300', '-o', image_page_path, '-dNOPAUSE',
                   '-dSAFER', '-c', '67108864', 'setvmthreshold', '-dGraphicsAlphaBits=4', '-dTextAlphaBits=4', '-f',
                   document_path]  # extract the page as an image
        output = _get_subprocess_output(cmdline, shell=False)
        output = output.decode('ascii', errors='ignore')

        if os.path.getsize(image_page_path) == 0:
            raise Exception('Ghostscript image extraction failed with output:\n{}'.format(output))

        results = image_to_text(image_page_path, create_searchable_pdf=create_searchable_pdf)

    finally:
        os.remove(image_page_path)

    results['page'] = page

    return results


# end def


def image_to_text(document_path, create_searchable_pdf=True):
    _, ext = os.path.splitext(document_path)
    ext = ext.lower()

    cmdline = [os.path.join(BIN_DIR, 'tesseract'), document_path, document_path, '-l', 'eng', '-psm', '1',
               '--tessdata-dir', os.path.join(LIB_DIR, 'tesseract')]
    if create_searchable_pdf:
        cmdline += ['pdf']

    _get_subprocess_output(cmdline, shell=False, env=dict(LD_LIBRARY_PATH=os.path.join(LIB_DIR, 'tesseract')))

    if create_searchable_pdf:
        searchable_pdf_path = document_path + '.pdf'
        text = _pdf_to_text(searchable_pdf_path)
    else:
        searchable_pdf_path = None
        with io.open(document_path + '.txt', mode='r', encoding='utf-8', errors='ignore') as f:
            text = f.read().strip()
    # end def

    return dict(success=True, text=text, method='image_to_text', searchable_pdf_path=searchable_pdf_path)


# end def


PARSE_FUNCS = {
    '.pdf': pdf_to_text_with_ocr,
    '.png': image_to_text,
    '.tiff': image_to_text,
    '.tif': image_to_text,
    '.jpg': image_to_text,
    '.jpeg': image_to_text,
}
