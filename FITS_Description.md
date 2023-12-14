A FITS (Flexible Image Transport System) file is a standard data format used in astronomy, primarily for storing image data, such as from telescopes or other astronomical observations, but also for storing tables of data or arrays of spectra. The structure of a FITS file is segmented into "Header-Data Units" (HDUs), where each HDU consists of a header and the subsequent data.

### Typical Structure of FITS File Header:

1. **Header**:
   - The header of each HDU is a sequence of ASCII text records (each 80 characters long) in a keyword=value format. 
   - It contains metadata describing the data in that HDU, such as size, format, and other properties.
   - The header always starts with a `SIMPLE` keyword indicating it's a standard FITS file.
   - Common keywords include:
     - `BITPIX`: Number of bits per data pixel.
     - `NAXIS`: Number of data array dimensions.
     - `NAXISn`: Size of each data array dimension (where `n` is the dimension number, e.g., `NAXIS1`, `NAXIS2`).
     - `EXTEND`: Indicates whether the file may contain extensions.
     - `BSCALE` and `BZERO`: Scaling factors for the data values.
     - Observation details like `DATE-OBS` (observation date), `EXPTIME` (exposure time), `TELESCOP` (telescope name), etc.
   - The header ends with an `END` keyword.

2. **Data**:
   - The data portion follows the header and can contain various types of data, like images (2D arrays), tables, or spectra.
   - The data format and layout are described by the header keywords.

3. **Multiple HDUs**:
   - FITS files can contain multiple HDUs, each with its own header and data section.
   - The first HDU is called the "Primary HDU" or "Primary Array". Additional HDUs are called "Extensions". Extensions can be image extensions, table extensions (ASCII or binary), etc.

4. **Comment and History Keywords**:
   - Headers often include `COMMENT` and `HISTORY` keywords, which are used to provide additional information about the file or data processing history.

5. **World Coordinate System (WCS)**:
   - FITS headers may include WCS information, which maps the pixel coordinates to celestial coordinates.

6. **Flexible and Customizable**:
   - FITS is designed to be flexible, so headers can contain a wide range of additional keywords specific to particular datasets or scientific needs.

FITS headers are a key feature of the format because they ensure that all the necessary information to interpret the data correctly is stored right with the data. This self-describing nature is one of the reasons FITS has been a durable and popular format in astronomy.