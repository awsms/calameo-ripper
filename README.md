# calameo-ripper

Toolkit to download Calameo publications and export them to PDF, SVG/SVGZ, or JPG.

## Quick start

```bash
go run . 'https://www.calameo.com/read/0004132596a81e123a0a9'
```

## Defaults

- `-formats=pdf`
- `-pdf-source=svgz`
- `-svg-renderer=auto`
- `-embed-ocr=true`
- `-ocr-sort=line`
- `-ocr-use-svg-size=true`
- `-ocr-fit=page`
- `-ocr-placement=simple`
- `-ocr-flip-y=true` (set `false` if the text is vertically inverted)

## Common flags

- `-o <file>`: output PDF path.
- `-formats pdf,jpg,svg,svgz`: select outputs.
- `-pdf-source svgz|jpg`: source for PDF generation.
- `-embed-ocr`: toggle OCR overlay.
- `-ocr-sort source|line|pdf`: OCR ordering.
- `-ocr-use-svg-size`: use SVG dimensions for OCR scaling.
- `-ocr-fit page|bbox`: OCR scaling mode.
- `-ocr-placement simple|matrix`: OCR placement mode.

## Examples

```bash
go run . -o book.pdf -formats pdf,jpg 'https://www.calameo.com/books/000413259473d01615745'
go run . 0004132596a81e123a0a9 00041325919b833642cb9
```

Raw page downloads are written to `./<title>-assets` by default, or to `-outdir` if provided.