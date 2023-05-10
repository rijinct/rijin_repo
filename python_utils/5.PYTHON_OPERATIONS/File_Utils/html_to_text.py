import html2text

h = html2text.HTML2Text()
h.ignore_links = True
print (h.handle("""<?xml version="1.0" encoding="UTF-8"?>
                                                    <html xmlns="http://www.w3.org/1999/xhtml">
                                                                <head>
                                                                <meta name="description" content=""/>
                                                                                                 <meta name="keywords" content=""/>
                                                                                                                               <meta name="author" content=""/>
                                                                                                                                                           <meta name="creator" content=""/>
                                                                                                                                                                                        <meta name="producer" content=""/>
                                                                                                                                                                                                                      <meta name="pages" content="16"/>
                                                                                                                                                                                                                                                 <meta name="created" content=""/>
                                                                                                                                                                                                                                                                              <meta name="modified" content=""/>
                                                                                                                                                                                                                                                                                                            <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                                                                                                                                                                                                                                                                                                                                                    <link rel="stylesheet" type="text/css" href="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/conv.css"/>
                                                                                                                                                                                                                                                                                                                                                                                                <link rel="toc" type="text/html" href="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/toc.xml"/>
                                                                                                                                                                                                                                                                                                                                                                                                                                      <title></title>
                                                                                                                                                                                                                                                                                                                                                                                                                                               <style type="text/css">


                                                                                                                                                                                                                                                                                                                                                                                                                                                           h1, h2, h3, h4, h5, h6 {
    font-size: 100%;
font-weight: normal;
font-style: normal
}

</style>
  <style type="text/css">
/**/
.c168 { text-align: justify; line-height: 9.2pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 294.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c167 { text-align: left; line-height: 13.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 453.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c166 { text-align: left; line-height: 13.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 294.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c165 { text-align: left; line-height: 13.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 219.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c164 { text-align: left; line-height: 15.9pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 453.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c163 { text-align: left; line-height: 15.9pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 294.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c162 { text-align: left; line-height: 15.9pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 219.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c161 { text-align: left; line-height: 17.7pt; text-indent: 0pt; margin-left: 1pt; margin-right: 0pt; padding-left: 59.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c160 { text-align: left; line-height: 13.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 380.25pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c159 { text-align: left; line-height: 13.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 60.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c158 { text-align: left; line-height: 15.9pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 380.25pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c157 { text-align: left; line-height: 24.8pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 60.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c156 { text-align: left; line-height: 9.1pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 61.25pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c155 { text-align: justify; line-height: 12.6pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 61.25pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c154 { text-align: left; line-height: 15.9pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 304.25pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c153 { text-align: left; line-height: 13.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 304.25pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c152 { text-align: justify; line-height: 9.2pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 60.5pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c151 { text-align: left; line-height: 9.1pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 304.25pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c150 { text-align: left; line-height: 15.9pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 60.5pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c149 { text-align: justify; line-height: 9.1pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 60.5pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c148 { text-align: justify; line-height: 9.2pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 304.25pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c147 { text-align: justify; line-height: 9.1pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 304.25pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c146 { text-align: left; line-height: 13.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 504.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c145 { text-align: left; line-height: 13.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 261pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c144 { text-align: left; line-height: 13.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 60.5pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c143 { text-align: left; line-height: 15.9pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 504.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c142 { text-align: justify; line-height: 12.6pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 304.25pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c141 { text-align: left; line-height: 15.9pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 261pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c140 { text-align: justify; line-height: 12.6pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 60.5pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c139 { text-align: left; line-height: 17.7pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 304.25pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c138 { text-align: left; line-height: 17.7pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 60.5pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c137 { text-align: justify; line-height: 9.1pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 61.25pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c136 { text-align: left; line-height: 9.1pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 305pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c135 { text-align: left; line-height: 21.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 305pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c134 { text-align: justify; line-height: 9.1pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 305pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c133 { text-align: left; line-height: 15.9pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 61.25pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c132 { text-align: justify; line-height: 9.2pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 305pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c131 { text-align: justify; line-height: 9.2pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 61.25pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c130 { text-align: left; line-height: 13.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 505.5pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c129 { text-align: left; line-height: 13.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 305pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c128 { text-align: left; line-height: 15.9pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 305pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c127 { text-align: left; line-height: 15.9pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 305.25pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c126 { text-align: left; line-height: 13.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 261.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c125 { text-align: left; line-height: 13.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 61.25pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c124 { text-align: left; line-height: 15.9pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 505.5pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c123 { text-align: left; line-height: 15.9pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 261.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c122 { text-align: left; line-height: 17.7pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 305pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c121 { text-align: left; line-height: 17.7pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 61.25pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c120 { text-align: justify; line-height: 10.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 58pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c119 { text-align: left; line-height: 15.9pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 58pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c118 { text-align: left; line-height: 12.1pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 58pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c117 { text-align: center; line-height: 5.1pt; text-indent: 0pt; margin-left: 44pt; margin-right: 0pt; padding-left: 22.25pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c116 { text-align: justify; line-height: 9.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 58pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c115 { text-align: left; line-height: 15.0pt; text-indent: 0pt; margin-left: 3pt; margin-right: 0pt; padding-left: 57.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c114 { text-align: justify; line-height: 12.3pt; text-indent: 0pt; margin-left: 3pt; margin-right: 0pt; padding-left: 57.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c113 { text-align: center; line-height: 10.3pt; text-indent: 0pt; margin-left: 4pt; margin-right: 0pt; padding-left: 493.65pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c112 { text-align: left; line-height: 15.9pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 466.25pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c111 { text-align: center; line-height: 10.3pt; text-indent: 0pt; margin-left: 4pt; margin-right: 0pt; padding-left: 413.9pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c110 { text-align: left; line-height: 15.9pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 386.25pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c109 { text-align: center; line-height: 10.3pt; text-indent: 0pt; margin-left: 4pt; margin-right: 0pt; padding-left: 333.9pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c108 { text-align: left; line-height: 15.9pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 306.25pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c107 { text-align: center; line-height: 10.3pt; text-indent: 0pt; margin-left: 4pt; margin-right: 0pt; padding-left: 253.9pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c106 { text-align: left; line-height: 21.1pt; text-indent: 0pt; margin-left: 4pt; margin-right: 0pt; padding-left: 177.5pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c105 { text-align: left; line-height: 15.9pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 177.9pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c104 { text-align: left; line-height: 21.1pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 101.5pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c103 { text-align: left; line-height: 21.1pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 66.25pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c102 { text-align: left; line-height: 24.8pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 58pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c101 { text-align: left; line-height: 14.2pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 58.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c100 { text-align: justify; line-height: 9.1pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 58.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c99 { text-align: justify; line-height: 9.2pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 341.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c98 { text-align: justify; line-height: 9.1pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 341.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c97 { text-align: left; line-height: 22.2pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 60.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c96 { text-align: left; line-height: 18.6pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 60.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c95 { text-align: left; line-height: 14.2pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 341.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c94 { text-align: left; line-height: 17.7pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 265pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c93 { text-align: left; line-height: 14.2pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 60.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c92 { text-align: left; line-height: 15.9pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 341.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c91 { text-align: justify; line-height: 10.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 265.25pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c90 { text-align: left; line-height: 15.9pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 265.25pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c89 { text-align: left; line-height: 15.0pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 58pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c88 { text-align: left; line-height: 15.0pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 251.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c87 { text-align: left; line-height: 17.7pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 251.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c86 { text-align: left; line-height: 17.7pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 101.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c85 { text-align: justify; line-height: 12.6pt; text-indent: 0pt; margin-left: 3pt; margin-right: 0pt; padding-left: 57.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c84 { text-align: left; line-height: 16.6pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 58.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c83 { text-align: left; line-height: 28.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 58.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c82 { text-align: justify; line-height: 9.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 58.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c81 { text-align: left; line-height: 14.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 206.25pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c80 { text-align: justify; line-height: 10.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 87.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c79 { text-align: left; line-height: 15.0pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 281.25pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c78 { text-align: left; line-height: 14.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 191.25pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c77 { text-align: left; line-height: 15.0pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 87.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c76 { text-align: left; line-height: 17.7pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 266.25pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c75 { text-align: justify; line-height: 12.6pt; text-indent: 0pt; margin-left: 1pt; margin-right: 0pt; padding-left: 189.3pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c74 { text-align: left; line-height: 17.7pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 87.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c73 { text-align: left; line-height: 17.7pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 60.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c72 { text-align: left; line-height: 11.4pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 60pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c71 { width: 723.00pt; height: 360.00pt; clip: rect(0pt, 723.00pt, 360.00pt, 0pt); margin-left: 58.00pt; margin-top: 0; margin-bottom: 0; margin-right: 0; padding: 0 0 0 0; z-index: 00;}
.c70 { text-align: left; line-height: 15.0pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 306.5pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c69 { text-align: justify; line-height: 10.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 61.5pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c68 { text-align: justify; line-height: 10.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 306.5pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c67 { text-align: left; line-height: 10.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 306.5pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c66 { text-align: justify; line-height: 10.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 206.5pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c65 { text-align: left; line-height: 10.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 61.5pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c64 { text-align: left; line-height: 15.0pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 156.5pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c63 { text-align: left; line-height: 15.0pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 126.5pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c62 { text-align: left; line-height: 15.9pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 306.5pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c61 { text-align: left; line-height: 15.9pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 206.5pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c60 { text-align: left; line-height: 15.9pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 154.5pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c59 { text-align: left; line-height: 15.9pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 126.5pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c58 { text-align: left; line-height: 15.9pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 61.5pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c57 { width: 723.00pt; height: 291.00pt; clip: rect(0pt, 723.00pt, 291.00pt, 0pt); margin-left: 58.75pt; margin-top: 0; margin-bottom: 0; margin-right: 0; padding: 0 0 0 0; z-index: 00;}
.c56 { text-align: justify; line-height: 11.4pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 58pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c55 { text-align: left; line-height: 15.0pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 305.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c54 { text-align: justify; line-height: 10.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 305.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c53 { text-align: left; line-height: 10.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 60.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c52 { text-align: left; line-height: 15.0pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 60.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c51 { text-align: left; line-height: 10.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 305.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c50 { text-align: justify; line-height: 10.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 205.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c49 { text-align: left; line-height: 15.0pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 155.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c48 { text-align: left; line-height: 15.0pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 125.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c47 { text-align: justify; line-height: 10.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 60.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c46 { text-align: left; line-height: 15.9pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 305.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c45 { text-align: left; line-height: 15.9pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 205.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c44 { text-align: left; line-height: 15.9pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 153.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c43 { text-align: left; line-height: 15.9pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 125.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c42 { text-align: left; line-height: 15.9pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 60.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c41 { text-align: left; line-height: 10.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 58pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c40 { width: 723.00pt; height: 507.00pt; clip: rect(0pt, 723.00pt, 507.00pt, 0pt); margin-left: 58.00pt; margin-top: 0; margin-bottom: 0; margin-right: 0; padding: 0 0 0 0; z-index: 00;}
.c39 { text-align: left; line-height: 28.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 58pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c38 { text-align: left; line-height: 16.6pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 60.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c37 { text-align: left; line-height: 21.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 60.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c36 { text-align: justify; line-height: 11.5pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 60.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c35 { text-align: left; line-height: 28.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 60.75pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c34 { text-align: left; line-height: 11.5pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 58pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c33 { text-align: left; line-height: 28.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 60pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c32 { text-align: left; line-height: 10.0pt; text-indent: 0pt; margin-left: 144pt; margin-right: 1pt; padding-left: 366pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c31 { width: 87.00pt; height: 27.00pt; clip: rect(0pt, 87.00pt, 27.00pt, 0pt); margin-left: 468.40pt; margin-top: 0; margin-bottom: 0; margin-right: 0; padding: 0 0 0 0; z-index: 00;}
.c30 { text-align: left; line-height: 15.0pt; text-indent: 0pt; margin-left: 144pt; margin-right: 144pt; padding-left: 132pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c29 { text-align: left; line-height: 15.0pt; text-indent: 0pt; margin-left: 144pt; margin-right: 0pt; padding-left: 291.25pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c28 { text-align: left; line-height: 11.5pt; text-indent: 0pt; margin-left: 4pt; margin-right: 0pt; padding-left: 57.5pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c27 { text-align: left; line-height: 16.7pt; text-indent: 0pt; margin-left: 0pt; margin-right: 99pt; padding-left: 58pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c26 { text-align: left; line-height: 24.8pt; text-indent: 0pt; margin-left: 4pt; margin-right: 144pt; padding-left: 57.5pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c25 { text-align: left; line-height: 24.8pt; text-indent: 0pt; margin-left: 2pt; margin-right: 144pt; padding-left: 58pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c24 { text-align: left; line-height: 15.0pt; text-indent: 0pt; margin-left: 2pt; margin-right: 144pt; padding-left: 58pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c23 { text-align: left; line-height: 10.0pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 510pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c22 { text-align: left; line-height: 15.0pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 276pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c21 { width: 87.00pt; height: 27.00pt; clip: rect(0pt, 87.00pt, 27.00pt, 0pt); margin-left: 468.40pt; margin-top: 0; margin-bottom: 0; margin-right: 0; padding: 0 0 0 0; z-index: 01;}
.c20 { text-align: left; line-height: 16.6pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 58pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c19 { width: 723.00pt; height: 696.00pt; clip: rect(0pt, 723.00pt, 696.00pt, 0pt); margin-left: 58.75pt; margin-top: 0; margin-bottom: 0; margin-right: 0; padding: 0 0 0 0; z-index: 00;}
.c18 { text-align: justify; line-height: 11.5pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 58pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c17 { text-align: left; line-height: 17.7pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 58pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c16 { text-align: left; line-height: 16.7pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 58pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c15 { text-align: left; line-height: 16.7pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 60pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c14 { text-align: left; line-height: 24.8pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 60pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c13 { text-align: justify; line-height: 59.8pt; text-indent: 2pt; margin-left: 0pt; margin-right: 0pt; padding-left: 58pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c12 { text-align: left; line-height: 15.0pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 435.25pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c11 { text-align: left; line-height: 15.0pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 60pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c10 { text-align: left; line-height: 10.0pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 518.25pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c9 { text-align: left; line-height: 13.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 181.5pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c8 { text-align: justify; line-height: 9.2pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 181.5pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c7 { width: 169.50pt; height: 34.50pt; clip: rect(0pt, 169.50pt, 34.50pt, 0pt); margin-left: 58.00pt; margin-top: 0; margin-bottom: 0; margin-right: 0; padding: 0 0 0 0; z-index: 02;}
.c6 {text-align: justify; line-height: 11.5pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 60pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c5 {width: 583.50pt; height: 258.00pt; clip: rect(0pt, 583.50pt, 258.00pt, 0pt); margin-left: 58.00pt; margin-top: 0; margin-bottom: 0; margin-right: 0; padding: 0 0 0 0; z-index: 01;}
.c4 {text-align: left; line-height: 28.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 59.5pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c3 {text-align: left; line-height: 28.3pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 60pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c2 {text-align: left; line-height: 20.0pt; text-indent: 0pt; margin-left: 0pt; margin-right: 0pt; padding-left: 59.5pt; padding-top: 0; padding-bottom: 0; padding-right: 0; z-index: 100;}
.c1 {width: 379.50pt; height: 108.00pt; clip: rect(0pt, 379.50pt, 108.00pt, 0pt); margin-left: 58.00pt; margin-top: 0; margin-bottom: 0; margin-right: 0; padding: 0 0 0 0; z-index: 00;}
/**/
</style>
  </head>
    <body class="font-1">
<span class="pageStart" id="pgs0001"> </span>
<div class="c1"><img src="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0001_00.jpg" width="379.50pt" height="108.00pt" border="0" alt="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0001_00.jpg(1517x432)"/></div>
<div class="mlsection3"><h3 class="c2 textStyle0"> <a name="t0" id="t0"></a>By C. Keith Conners, Ph.D.</h3>
</div><div class="mlsection3"><h3 class="c3 textStyle3"> <a name="t2" id="t2"></a>Parent</h3>
<p class="c4 textStyle3">Assessment Report</p>
<div class="c5"><img src="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0001_01.jpg" width="583.50pt" height="258.00pt" border="0" alt="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0001_01.jpg(2331x1031)"/></div>
<div class="mlsection4"><h4></h4><p class="c6 textStyle2"> <a name="t3" id="t3"></a>This Assessment Report is intended for use by qualified assessors only, and is not to be shown or in any
other way provided to the respondent or any other unqualified individuals.</p>
<div class="c7"><img src="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0001_02.jpg" width="169.50pt" height="34.50pt" border="0" alt="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0001_02.jpg(675x135)"/></div>
<p class="c8 textStyle1">Copyright © 2009 Multi-Health Systems Inc. All rights reserved.
P.O. Box 950, North Tonawanda, NY 14120-0950</p>
<p class="c9 textStyle1">3770 Victoria Park Ave., Toronto, ON M2H 3M6</p>
<p class="c10 textStyle4">ver. 1.2</p>
<span class="pageEnd" id="pge0001"> </span> <span class="pageStart" id="pgs0002"> </span>
<p class="c11 textStyle8">Conners Early Childhood–P Assessment Report for Thea Lacheta (101677)</p>
<p class="c12 textStyle8">Admin Date: 02/03/2022</p>
<div class="mlsection6"><h6 class="c13"><a name="t7" id="t7"></a><span class="textStyle3">Summary of Results
</span><span class="textStyle5"> <a name="t7" id="t7"></a>Overview of Scores</span></h6>
</div><div class="mlsection6"><h6 class="c14 textStyle5"> <a name="t6" id="t6"></a>Response Style Analysis</h6>
<p class="c15 textStyle2">Scores on the Validity scales do not indicate a positive, negative, or inconsistent response style.</p>
<p class="c16"><span class="textStyle2">The following graph provides </span><span class="textStyle6">T</span><span class="textStyle2">-scores for each of the Conners Early Childhood–P scales. </span></p>
<a name="t9" id="t9"></a>
<div class="c19"><img src="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0002_00.jpg" width="723.00pt" height="696.00pt" border="0" alt="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0002_00.jpg(2889x2781)"/>
<p class="c17 textStyle7">Note:</p>
<p class="c18 textStyle2">Defiance/Temper and Aggression are subscales of the Defiant/Aggressive Behaviors scale. Social
<a name="t10" id="t10"></a>Functioning and Atypical Behaviors are subscales of the Social Functioning/Atypical Behaviors scale. Sleep </p>
</div>
<p class="c20 textStyle2">Problems is a subscale of the Physical Symptoms scale. </p>
<p class="c11 textStyle8">Copyright © 2009 Multi-Health Systems Inc. All rights reserved.</p>
<div class="c21"><img src="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0002_01.jpg" width="87.00pt" height="27.00pt" border="0" alt="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0002_01.jpg(347x108)"/></div>
<p class="c22 textStyle8">Page 2</p>
<p class="c23 textStyle4">ver. 1.2</p>
<span class="pageEnd" id="pge0002"> </span> <span class="pageStart" id="pgs0003"> </span>
<p class="c24 textStyle8">Conners Early Childhood–P Assessment Report for Thea Lacheta (101677)</p>
</div></div></div><div class="mlsection1"><h1 class="c25 textStyle5"> <a name="t13" id="t13"></a>Summary of Elevated Scores</h1>
</div><div class="mlsection1"><h1 class="c26 textStyle5"> <a name="t12" id="t12"></a>Cautionary Remarks</h1>
<p class="c27 textStyle2">The parent’s ratings of Thea Lacheta (101677) did not result in any elevated scores. </p>
<p class="c28 textStyle2">This Summary of Results section provides information only about areas that are a concern. Please refer to
the remainder of the Assessment Report for further information regarding areas that are not elevated or
could not be scored due to too many omitted items.</p>
<p class="c29 textStyle8">Admin Date: 02/03/2022</p>
<p class="c24 textStyle8">Copyright © 2009 Multi-Health Systems Inc. All rights reserved.</p>
<p class="c30 textStyle8">Page 3</p>
<div class="c31"><img src="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0003_00.jpg" width="87.00pt" height="27.00pt" border="0" alt="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0003_00.jpg(347x108)"/></div>
<p class="c32 textStyle4">ver. 1.2</p>
<span class="pageEnd" id="pge0003"> </span> <span class="pageStart" id="pgs0004"> </span>
<p class="c11 textStyle8">Conners Early Childhood–P Assessment Report for Thea Lacheta (101677)</p>
<p class="c12 textStyle8">Admin Date: 02/03/2022</p>
<p class="c33 textStyle3"> <a name="t14" id="t14"></a>Introduction</p>
<p class="c34"><span class="textStyle2">The Conners Early Childhood–Parent (Conners Early Childhood–P) is an assessment tool used to obtain a
parent’s observations about his or her child’s behavior. This instrument is designed to assess a wide range
of behavioral, emotional, social and developmental issues in young children. When used in combination with
    other information, results from the Conners Early Childhood–P can provide valuable information to aid in
assessment and guide intervention decisions. This report provides information about the parent's 
assessment of the child, how he/she compares to other children, and which scales are elevated. See the
</span><span class="textStyle6">Conners Early Childhood Manual</span><span class="textStyle2"> (published by MHS) for more information. </span></p>
<p class="c34 textStyle2">This computerized report is an interpretive aid and should not be given to parents or other unqualified users,
or used as the sole criterion for clinical decision-making. Administrators are cautioned against drawing
unsupported interpretations. Combining information from this report with information gathered from other
psychometric measures, interviews, observations, and review of available records will give the assessor a
more comprehensive view of the child than might be obtained from any one source. This report is based on
an algorithm that produces the most common interpretations for the obtained scores. Administrators should
<a name="t15" id="t15"></a>review parent’s responses to specific items to ensure that these interpretations apply to the child being </p>
<p class="c16 textStyle2">described. </p>
<p class="c35 textStyle3"> <a name="t17" id="t17"></a>Response Style Analysis</p>
<p class="c36 textStyle2">The following section provides the parent’s scores for the Positive and Negative Impression scales and the
Inconsistency Index.</p>
<p class="c37 textStyle9"> <a name="t19" id="t19"></a>Positive Impression</p>
<p class="c38 textStyle2">The Positive Impression score (raw score = 0) does not indicate an overly positive response style.</p>
<p class="c37 textStyle9"> <a name="t21" id="t21"></a>Negative Impression</p>
<p class="c38 textStyle2">The Negative Impression score (raw score = 0) does not indicate an overly negative response style.</p>
<p class="c37 textStyle9"> <a name="t23" id="t23"></a>Inconsistency Index</p>
<p class="c38 textStyle2">The Inconsistency Index score (raw score = 0) does not indicate an inconsistent response style.</p>
<p class="c11 textStyle8">Copyright © 2009 Multi-Health Systems Inc. All rights reserved.</p>
<div class="c31"><img src="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0004_00.jpg" width="87.00pt" height="27.00pt" border="0" alt="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0004_00.jpg(347x108)"/></div>
<p class="c22 textStyle8">Page 4</p>
<p class="c23 textStyle4">ver. 1.2</p>
<span class="pageEnd" id="pge0004"> </span> <span class="pageStart" id="pgs0005"> </span>
<p class="c11 textStyle8">Conners Early Childhood–P Assessment Report for Thea Lacheta (101677)</p>
<p class="c12 textStyle8">Admin Date: 02/03/2022</p>
<p class="c39"><span class="textStyle3"> <a name="t24" id="t24"></a>Behavior Scales: </span><span class="textStyle10">T</span><span class="textStyle3">-scores</span></p>
<p class="c16"><span class="textStyle2"> <a name="t25" id="t25"></a>The following graph provides </span><span class="textStyle6">T</span><span class="textStyle2">-scores for each of the scales. </span></p>
<div class="c40"><img src="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0005_00.jpg" width="723.00pt" height="507.00pt" border="0" alt="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0005_00.jpg(2894x2025)"/></div>
<p class="c11 textStyle8">Copyright © 2009 Multi-Health Systems Inc. All rights reserved.</p>
<div class="c21"><img src="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0005_01.jpg" width="87.00pt" height="27.00pt" border="0" alt="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0005_01.jpg(347x108)"/></div>
<p class="c22 textStyle8">Page 5</p>
<p class="c23 textStyle4">ver. 1.2</p>
<span class="pageEnd" id="pge0005"> </span> <span class="pageStart" id="pgs0006"> </span>
<p class="c11 textStyle8"> <a name="t26" id="t26"></a>Conners Early Childhood–P Assessment Report for Thea Lacheta (101677)</p>
<p class="c12 textStyle8">Admin Date: 02/03/2022</p>
<p class="c39 textStyle3">Behavior Scales: Detailed Scores</p>
<p class="c41"><span class="textStyle8">The following table summarizes the results of the parent’s assessment of Thea Lacheta (101677), and provides general
information about how she compares to the normative group. Please refer to the </span><span class="textStyle11">Conners Early Childhood Manual</span><span class="textStyle8"> for
<a name="t30" id="t30"></a>more interpretation information.</span></p>
<p class="c42 textStyle12"> <a name="t27" id="t27"></a>Scale</p>
<p class="c43 textStyle12"> <a name="t29" id="t29"></a>Raw</p>
<p class="c43 textStyle12">Score</p>
<p class="c44"><br/>
<span class="textStyle13">T</span><span class="textStyle12">-score</span></p>
<p class="c45 textStyle12"> <a name="t31" id="t31"></a>Guideline</p>
<p class="c46 textStyle12"> <a name="t32" id="t32"></a>Common Characteristics of High Scorers</p>
<p class="c47 textStyle8">Inattention/
Hyperactivity</p>
<p class="c48 textStyle8">6</p>
<p class="c49 textStyle8">41</p>
<p class="c50 textStyle8">Average Score (Typical
levels of concern)</p>
<p class="c51 textStyle8">Difficulty with control of attention and/or behavior. May
have poor concentration or be easily distracted. May
lose interest quickly or have difficulty finishing things.
May have high activity levels and difficulty staying
seated. May be easily excited, impulsive and/or fidgety.</p>
<p class="c50 textStyle8">Average Score (Typical
levels of concern)</p>
<p class="c52 textStyle8">Defiant/</p>
<p class="c53 textStyle8">Aggressive
Behaviors
(D/A): Total</p>
<p class="c51 textStyle8">May be argumentative, defiant, destructive, or
dishonest. May have problems with controlling temper.
May have problems with physical and/or verbal
aggression.</p>
<p class="c50 textStyle8">Average Score (Typical
levels of concern)</p>
<p class="c48 textStyle8">6</p>
<p class="c49 textStyle8">45</p>
<p class="c52 textStyle8">Defiance/</p>
<p class="c47 textStyle8">Temper (D/A
subscale)</p>
<p class="c54 textStyle8">Difficult. May be argumentative, stubborn, and/or
defiant. May be manipulative, moody, whiny, or have
poor anger control.</p>
<p class="c50 textStyle8">Average Score (Typical
levels of concern)</p>
<p class="c48 textStyle8">6</p>
<p class="c49 textStyle8">46</p>
<p class="c47 textStyle8">Aggression
(D/A subscale)</p>
<p class="c48 textStyle8">0</p>
<p class="c49 textStyle8">43</p>
<p class="c54 textStyle8">Aggressive. May fight or bully. May be rude,
destructive, and/or dishonest.</p>
<p class="c50 textStyle8">Average Score (Typical
levels of concern)</p>
<p class="c53 textStyle8">Social
Functioning/
Atypical
Behaviors
(SF/AB): Total</p>
<p class="c51 textStyle8">Poor and/or odd, unusual social skills. May have
difficulty with friendships; socially awkward. May
appear disinterested in social interactions. May have
difficulty with emotions. May have unusual interests,
behaviors and/or language. May show repetitive or
rigid behavior.</p>
<p class="c50 textStyle8">Average Score (Typical
levels of concern)</p>
<p class="c48 textStyle8">11</p>
<p class="c49 textStyle8">55</p>
<p class="c53 textStyle8">Social
Functioning
(SF/AB
 subscale)</p>
<p class="c51 textStyle8">Poor social skills. May have difficulty with body
language, social cues, or emotions. May seem rude or
unfriendly. May have no friends; may be unliked,
unaccepted, or ignored by peers.</p>
<p class="c50 textStyle8">Average Score (Typical
levels of concern)</p>
<p class="c48 textStyle8">8</p>
<p class="c49 textStyle8">56</p>
<p class="c53 textStyle8">Atypical
Behaviors
(SF/AB
 subscale)</p>
<p class="c51 textStyle8">Odd and unusual. May have unusual interests and/or
language. May have repetitive body movements or
play. May be rigid or inflexible. May appear
disinterested in social interactions. May have limited
emotional expression. May engage in unusual
behaviors (e.g., self-harm, pica, tics).</p>
<p class="c50 textStyle8">Average Score (Typical
levels of concern)</p>
<p class="c48 textStyle8">3</p>
<p class="c49 textStyle8">52</p>
<p class="c51 textStyle8">Anxious, including emotional or physical symptoms.
May be fearful or have difficulty controlling worries.
May be clingy or easily frightened; may cry easily.
Feelings may be easily hurt. May complain of<br/>aches/pains. May have sleep difficulties or nightmares.</p>
<p class="c50 textStyle8">Average Score (Typical
levels of concern)</p>
<p class="c52 textStyle8">Anxiety</p>
<p class="c48 textStyle8">7</p>
<p class="c49 textStyle8">50</p>
<p class="c47 textStyle8">Mood and
Affect</p>
<p class="c54 textStyle8">Mood problems may include irritability, sadness,
negativity, and anhedonia. May be tearful. May display
sad or morbid themes in play.</p>
<p class="c50 textStyle8">Average Score (Typical
levels of concern)</p>
<p class="c48 textStyle8">5</p>
<p class="c49 textStyle8">53</p>
<p class="c47 textStyle8">Physical
Symptoms
(PS): Total</p>
<p class="c51 textStyle8">Physical symptoms that may have medical/emotional
roots. May complain of aches/pains or feeling sick.
May have eating issues. May have sleep difficulties or
nightmares.</p>
<p class="c50 textStyle8">Average Score (Typical
levels of concern)</p>
<p class="c48 textStyle8">2</p>
<p class="c49 textStyle8">47</p>
<p class="c47 textStyle8">Sleep
Problems (PS
subscale)</p>
<p class="c48 textStyle8">2</p>
<p class="c49 textStyle8">54</p>
<p class="c55 textStyle8">May have sleep difficulties or nightmares.</p>
<div class="c31"><img src="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0006_00.jpg" width="87.00pt" height="27.00pt" border="0" alt="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0006_00.jpg(347x108)"/></div>
<p class="c11 textStyle8">Copyright © 2009 Multi-Health Systems Inc. All rights reserved.</p>
<p class="c22 textStyle8">Page 6</p>
<p class="c23 textStyle4">ver. 1.2</p>
<span class="pageEnd" id="pge0006"> </span> <span class="pageStart" id="pgs0007"> </span>
<p class="c11 textStyle8">Conners Early Childhood–P Assessment Report for Thea Lacheta (101677)</p>
<p class="c12 textStyle8">Admin Date: 02/03/2022</p>
<p class="c39 textStyle3"> <a name="t33" id="t33"></a>Conners Early Childhood Global Index</p>
<p class="c56 textStyle2">The following section summarizes the parent’s ratings of Thea Lacheta (101677) with respect to the
Conners Early Childhood Global Index (Conners Early Childhood GI). High scores on the Conners Early
<a name="t34" id="t34"></a>Childhood GI tend to describe a child who is moody or emotional. They may also describe a child who is </p>
<p class="c16 textStyle2">restless, impulsive, or inattentive. </p>
<div class="c57"><img src="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0007_00.jpg" width="723.00pt" height="291.00pt" border="0" alt="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0007_00.jpg(2889x1166)"/></div>
<p class="c58 textStyle12"> <a name="t35" id="t35"></a>Scale</p>
<p class="c59 textStyle12"> <a name="t37" id="t37"></a>Raw</p>
<p class="c59 textStyle12">Score</p>
<p class="c60"><br/>
<span class="textStyle13"> <a name="t38" id="t38"></a>T</span><span class="textStyle12">-score</span></p>
<p class="c61 textStyle12"> <a name="t39" id="t39"></a>Guideline</p>
<p class="c62 textStyle12"> <a name="t40" id="t40"></a>Common Characteristics of High Scorers</p>
<p class="c63 textStyle8">4</p>
<p class="c64 textStyle8">46</p>
<p class="c65 textStyle8">Conners Early
Childhood GI:
Restless-Impulsive</p>
<p class="c66 textStyle8">Average Score (Typical
levels of concern)</p>
<p class="c67 textStyle8">May be easily distracted. May be restless, fidgety, or
impulsive. May have trouble finishing things. May
distract others.</p>
<p class="c66 textStyle8">Average Score (Typical
levels of concern)</p>
<p class="c63 textStyle8">1</p>
<p class="c64 textStyle8">47</p>
<p class="c65 textStyle8">Conners Early
Childhood GI:
Emotional
Lability</p>
<p class="c68 textStyle8">Moody and emotional; may cry, lose temper, or
become frustrated easily.</p>
<p class="c63 textStyle8">5</p>
<p class="c64 textStyle8">46</p>
<p class="c69 textStyle8">Conners Early
Childhood GI:
Total</p>
<p class="c66 textStyle8">Average Score (Typical
levels of concern)</p>
<p class="c70 textStyle8">Moody and emotional; restless, impulsive, inattentive.</p>
<p class="c11 textStyle8">Copyright © 2009 Multi-Health Systems Inc. All rights reserved.</p>
<div class="c21"><img src="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0007_01.jpg" width="87.00pt" height="27.00pt" border="0" alt="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0007_01.jpg(347x108)"/></div>
<p class="c22 textStyle8">Page 7</p>
<p class="c23 textStyle4">ver. 1.2</p>
<span class="pageEnd" id="pge0007"> </span> <span class="pageStart" id="pgs0008"> </span>
<p class="c11 textStyle8">Conners Early Childhood–P Assessment Report for Thea Lacheta (101677)</p>
<p class="c12 textStyle8">Admin Date: 02/03/2022</p>
<p class="c39"><span class="textStyle3"> <a name="t41" id="t41"></a>Developmental Milestone Scales: </span><span class="textStyle10">T</span><span class="textStyle3">-scores</span></p>
<p class="c16"><span class="textStyle2"> <a name="t43" id="t43"></a>The following graph provides </span><span class="textStyle6">T</span><span class="textStyle2">-scores for each of the scales. High </span><span class="textStyle6">T</span><span class="textStyle2">-scores for the Developmental Milestone </span></p>
<p class="c16 textStyle2">scales suggest that the parent has concerns about Thea Lacheta (101677)’s development. </p>
<div class="c71"><img src="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0008_00.jpg" width="723.00pt" height="360.00pt" border="0" alt="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0008_00.jpg(2894x1440)"/></div>
<p class="c39 textStyle3"> <a name="t44" id="t44"></a>Developmental Milestone Scales: Detailed Scores</p>
<p class="c56"><span class="textStyle2">The following table summarizes the results of the parent’s assessment of Thea Lacheta (101677), and
provides general information about how she compares to the normative group. Please refer to the </span><span class="textStyle6">Conners
Early Childhood Manual</span><span class="textStyle2"> for more interpretation information. </span></p>
<p class="c42 textStyle12"> <a name="t45" id="t45"></a>Scale</p>
<p class="c43 textStyle12"> <a name="t47" id="t47"></a>Raw</p>
<p class="c43 textStyle12">Score</p>
<p class="c44"><br/>
<span class="textStyle13"> <a name="t48" id="t48"></a>T</span><span class="textStyle12">-score</span></p>
<p class="c45 textStyle12"> <a name="t49" id="t49"></a>Guideline</p>
<p class="c54 textStyle12"> <a name="t50" id="t50"></a>Common Characteristics of High Scorers
Potential delay in the development of…</p>
<p class="c54 textStyle8">Adaptive functioning skills, including dressing,
eating/drinking, toileting, personal hygiene, and
helping.</p>
<p class="c50 textStyle8">Average Score (Typical
levels of concern)</p>
<p class="c52 textStyle8">Adaptive Skills</p>
<p class="c48 textStyle8">17</p>
<p class="c49 textStyle8">55</p>
<p class="c50 textStyle8">Average Score (Typical
levels of concern)</p>
<p class="c52 textStyle8">Communication</p>
<p class="c48 textStyle8">7</p>
<p class="c49 textStyle8">47</p>
<p class="c54 textStyle8">Expressive and receptive language, including verbal,
facial, and gestural communication.</p>
<p class="c50 textStyle8">Low Score (Fewer
concerns than are
typically reported)</p>
<p class="c52 textStyle8">Motor Skills</p>
<p class="c48 textStyle8">12</p>
<p class="c49 textStyle8">37</p>
<p class="c55 textStyle8">Fine and gross motor skills.</p>
<p class="c50 textStyle8">Low Score (Fewer
concerns than are
typically reported)</p>
<p class="c52 textStyle8">Play</p>
<p class="c48 textStyle8">0</p>
<p class="c49 textStyle8">33</p>
<p class="c55 textStyle8">Imaginative and pretend play.</p>
<p class="c47 textStyle8">Pre-Academic/
Cognitive</p>
<p class="c48 textStyle8">18</p>
<p class="c49 textStyle8">48</p>
<p class="c50 textStyle8">Average Score (Typical
levels of concern)</p>
<p class="c51 textStyle8">Knowledge of pre-academic concepts (e.g., shapes,
colors, letters, numbers, and body parts), pre-reading
skills (e.g., rhyming, name recognition), and early
memory/reasoning skills.</p>
<p class="c11 textStyle8">Copyright © 2009 Multi-Health Systems Inc. All rights reserved.</p>
<div class="c21"><img src="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0008_01.jpg" width="87.00pt" height="27.00pt" border="0" alt="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0008_01.jpg(347x108)"/></div>
<p class="c22 textStyle8">Page 8</p>
<p class="c23 textStyle4">ver. 1.2</p>
<span class="pageEnd" id="pge0008"> </span> <span class="pageStart" id="pgs0009"> </span>
<p class="c11 textStyle8"> <a name="t51" id="t51"></a>Conners Early Childhood–P Assessment Report for Thea Lacheta (101677)</p>
<p class="c12 textStyle8">Admin Date: 02/03/2022</p>
<p class="c33 textStyle3">Other Clinical Indicators</p>
<p class="c72 textStyle2">The following table displays the results from the parent’s observations of Thea Lacheta (101677)’s behavior
with regard to specific items that are related to other clinical concerns. Endorsement of these items may
indicate the need for further investigation.</p>
<p class="c73 textStyle7"> <a name="t52" id="t52"></a>Item</p>
<p class="c74 textStyle7"> <a name="t53" id="t53"></a>Item Content</p>
<p class="c75 textStyle7"> <a name="t54" id="t54"></a>Parent's Rating
0 1 2 3 ?</p>
<p class="c76 textStyle7"> <a name="t55" id="t55"></a>Recommendation</p>
<p class="c52 textStyle8">B 7</p>
<p class="c77 textStyle8">Cruelty to Animals</p>
<p class="c78 textStyle14">ü</p>
<p class="c79 textStyle8">No need for further investigation indicated</p>
<p class="c77 textStyle8">Fire Setting</p>
<p class="c52 textStyle8">B 73</p>
<p class="c78 textStyle14">ü</p>
<p class="c79 textStyle8">No need for further investigation indicated</p>
<p class="c77 textStyle8">Perfectionism</p>
<p class="c52 textStyle8">B 102</p>
<p class="c78 textStyle14">ü</p>
<p class="c79 textStyle8">No need for further investigation indicated</p>
<p class="c77 textStyle8">Pica</p>
<p class="c52 textStyle8">B 11</p>
<p class="c78 textStyle14">ü</p>
<p class="c79 textStyle8">No need for further investigation indicated</p>
<p class="c80 textStyle8">Posttraumatic Stress
Disorder</p>
<p class="c52 textStyle8">B 94</p>
<p class="c78 textStyle14">ü</p>
<p class="c79 textStyle8">No need for further investigation indicated</p>
<p class="c77 textStyle8">Self-Injury</p>
<p class="c52 textStyle8">B 110</p>
<p class="c78 textStyle14">ü</p>
<p class="c79 textStyle8">No need for further investigation indicated</p>
<p class="c77 textStyle8">Specific Phobia</p>
<p class="c52 textStyle8">B 41</p>
<p class="c81 textStyle14">ü</p>
<p class="c79 textStyle8">No need for further investigation indicated</p>
<p class="c77 textStyle8">Stealing</p>
<p class="c52 textStyle8">B 57</p>
<p class="c78 textStyle14">ü</p>
<p class="c79 textStyle8">No need for further investigation indicated</p>
<p class="c77 textStyle8">Tics: motor</p>
<p class="c52 textStyle8">B 9</p>
<p class="c78 textStyle14">ü</p>
<p class="c79 textStyle8">No need for further investigation indicated</p>
<p class="c77 textStyle8">Tics: vocal</p>
<p class="c52 textStyle8">B 14</p>
<p class="c78 textStyle14">ü</p>
<p class="c79 textStyle8">No need for further investigation indicated</p>
<p class="c77 textStyle8">Trichotillomania</p>
<p class="c52 textStyle8">B 105</p>
<p class="c78 textStyle14">ü</p>
<p class="c79 textStyle8">No need for further investigation indicated</p>
<p class="c82"><span class="textStyle12">Parent’s rating: </span><span class="textStyle8"> 0 = Not true at all (Never, Seldom); 1 = Just a little true (Occasionally); 2 = Pretty much true (Often,
Quite a bit); 3 = Very much true (Very often, Very frequently); ? = Omitted Item.</span></p>
<p class="c83 textStyle3">Impairment</p>
<p class="c84 textStyle2"> <a name="t58" id="t58"></a>The parent’s report of Thea Lacheta (101677)’s level of impairment in learning/pre-academic, peer </p>
<p class="c84 textStyle2">interactions, and home settings is presented below. </p>
<p class="c73 textStyle7"> <a name="t59" id="t59"></a>Item</p>
<p class="c74 textStyle7"> <a name="t60" id="t60"></a>Item Content</p>
<p class="c75 textStyle7"> <a name="t61" id="t61"></a>Parent's Rating
0 1 2 3 ?</p>
<p class="c76 textStyle7"> <a name="t62" id="t62"></a>Guideline</p>
<p class="c52 textStyle8">IM 1</p>
<p class="c77 textStyle8">Learning/Pre-Academic</p>
<p class="c78 textStyle14">ü</p>
<p class="c79 textStyle8">Problems never seriously affect learning.</p>
<p class="c77 textStyle8">Peer Interactions</p>
<p class="c52 textStyle8">IM 2</p>
<p class="c78 textStyle14">ü</p>
<p class="c79 textStyle8">Problems never seriously affect interactions with other children.</p>
<p class="c77 textStyle8">Home Life</p>
<p class="c52 textStyle8">IM 3</p>
<p class="c78 textStyle14">ü</p>
<p class="c79 textStyle8">Problems never seriously affect home life.</p>
<p class="c82"><span class="textStyle12">Parent’s rating: </span><span class="textStyle8"> 0 = Not true at all (Never, Seldom); 1 = Just a little true (Occasionally); 2 = Pretty much true (Often,
Quite a bit); 3 = Very much true (Very often, Very frequently); ? = Omitted Item.</span></p>
<p class="c39 textStyle3"> <a name="t64" id="t64"></a>Additional Questions</p>
<p class="c20 textStyle2">The following section displays additional comments from the parent about Thea Lacheta (101677). </p>
<p class="c85 textStyle7"> <a name="t65" id="t65"></a>Item
Number</p>
<p class="c86 textStyle7"> <a name="t66" id="t66"></a>Item Content</p>
<p class="c87 textStyle7"> <a name="t67" id="t67"></a>Parent's Response</p>
<p class="c52 textStyle8">AQ 1 Additional concerns about your child</p>
<p class="c88 textStyle8">She has a great imagination. Loves to read and is very caring</p>
<p class="c88 textStyle8">No</p>
<p class="c52 textStyle8">AQ 2 Child's strengths or skills</p>
<p class="c11 textStyle8">Copyright © 2009 Multi-Health Systems Inc. All rights reserved.</p>
<div class="c31"><img src="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0009_00.jpg" width="87.00pt" height="27.00pt" border="0" alt="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0009_00.jpg(347x108)"/></div>
<p class="c22 textStyle8">Page 9</p>
<p class="c23 textStyle4">ver. 1.2</p>
<span class="pageEnd" id="pge0009"> </span> <span class="pageStart" id="pgs0010"> </span>
<p class="c11 textStyle8">Conners Early Childhood–P Assessment Report for Thea Lacheta (101677)</p>
<p class="c12 textStyle8">Admin Date: 02/03/2022</p>
<p class="c39 textStyle3"> <a name="t68" id="t68"></a>Conners Early Childhood–P Results and IDEA</p>
<p class="c41 textStyle8">The Conners Early Childhood–P provides information that may be useful to consider when determining whether a child
is eligible for early intervention or special education and related services under current U.S. federal statutes, such as the
Individuals with Disabilities Education Improvement Act of 2004 (IDEA 2004). The following table summarizes areas of
IDEA 2004 categorization that are typically considered when a particular score is elevated. The “At Risk; Follow-up
Recommended” column indicates which areas are elevated for Thea Lacheta (101677), suggesting the need for follow-</p>
<p class="c89 textStyle8">up to determine if she is eligible for services under IDEA 2004 in this particular area.       </p>
<p class="c89"> </p>
<p class="c41"><span class="textStyle8">The information in this table is based on IDEA 2004 and general interpretation/application of this federal law. Specific
state and local education agencies may have specific requirements that supersede these recommendations. The
assessor is reminded to review local policies that may impact decision making. An elevated score is not sufficient
justification for IDEA 2004 eligibility. The IDEA 2004 indicates that categorization is not required for provision of
services, particularly in the case of early intervention services. In most districts, a child qualifies for early intervention
    services if there is evidence that he/she is at risk for substantial delays if services are not provided (even if no
developmental delays or diagnoses have been documented yet). Please see the </span><span class="textStyle11">Conners Early Childhood Manual</span><span class="textStyle8"> for
further discussion of IDEA 2004.</span></p>
<p class="c42 textStyle12"> <a name="t69" id="t69"></a>Content Areas</p>
<p class="c90 textStyle12"> <a name="t71" id="t71"></a>At Risk;</p>
<p class="c91 textStyle12">Follow-up
Recommended</p>
<p class="c92 textStyle12"> <a name="t72" id="t72"></a>Possible IDEA Eligibility Category</p>
<p class="c42 textStyle13">Behavior Scales</p>
<p class="c93 textStyle15">Inattention/Hyperactivity</p>
<p class="c94"> </p>
<p class="c95 textStyle15">DD-Cognitive, DD-Emotional, ED, OHI</p>
<p class="c96 textStyle15">Defiant/Aggressive Behaviors (D/A): Total  </p>
<p class="c95 textStyle15">DD-Emotional, ED</p>
<p class="c93 textStyle15">Defiance/Temper (D/A subscale)</p>
<p class="c94"> </p>
<p class="c95 textStyle15">DD-Emotional, ED</p>
<p class="c93 textStyle15">Aggression (D/A subscale)</p>
<p class="c94"> </p>
<p class="c95 textStyle15"> <a name="t83" id="t83"></a>DD-Emotional, ED</p>
<p class="c97 textStyle15">Social Functioning/Atypical Behaviors (SF/AB):Total  </p>
<p class="c98 textStyle15">Autism, DD-Cognitive, DD-Emotional, DD-Social,
ED, MR/ID</p>
<p class="c93 textStyle15"> <a name="t86" id="t86"></a>Social Functioning (SF/AB subscale)</p>
<p class="c94"> </p>
<p class="c95 textStyle15"> <a name="t87" id="t87"></a>Autism, DD-Emotional, DD-Social, ED</p>
<p class="c93 textStyle15">Atypical Behaviors (SF/AB subscale)</p>
<p class="c94"> </p>
<p class="c99 textStyle15">Autism, DD-Cognitive, DD-Emotional, DD-Social,
ED, MR/ID</p>
<p class="c93 textStyle15">Anxiety</p>
<p class="c94"> </p>
<p class="c95 textStyle15">DD-Emotional, ED, OHI</p>
<p class="c93 textStyle15">Mood and Affect</p>
<p class="c94"> </p>
<p class="c95 textStyle15">DD-Emotional, ED</p>
<p class="c93 textStyle15">Physical Symptoms (PS): Total</p>
<p class="c94"> </p>
<p class="c95 textStyle15"> <a name="t95" id="t95"></a>DD-Emotional, ED, OHI</p>
<p class="c93 textStyle15">Sleep Problems (PS subscale)</p>
<p class="c94"> </p>
<p class="c95 textStyle15">DD-Emotional, ED, OHI</p>
<p class="c42 textStyle13">Developmental Milestone Scales</p>
<p class="c93 textStyle15">Adaptive Skills</p>
<p class="c94"> </p>
<p class="c95 textStyle15">DD-Adaptive, MR/ID</p>
<p class="c93 textStyle15">Communication</p>
<p class="c94"> </p>
<p class="c95 textStyle15">Autism, DD-Communication, S/L</p>
<p class="c93 textStyle15">Motor Skills</p>
<p class="c94"> </p>
<p class="c95 textStyle15">DD-Physical, OHI</p>
<p class="c93 textStyle15">Play</p>
<p class="c94"> </p>
<p class="c95 textStyle15"> <a name="t106" id="t106"></a>Autism, DD-Cognitive, MR/ID</p>
<p class="c93 textStyle15">Pre-Academic/Cognitive</p>
<p class="c94"> </p>
<p class="c95 textStyle15">DD-Cognitive, DD-Communication, MR/ID, LD, S/L</p>
<p class="c42 textStyle13">Other Clinical Indicators</p>
<p class="c93 textStyle15">Cruelty to Animals</p>
<p class="c94"> </p>
<p class="c95 textStyle15">ED</p>
<p class="c93 textStyle15">Fire Setting</p>
<p class="c94"> </p>
<p class="c95 textStyle15">ED</p>
<p class="c93 textStyle15">Perfectionism</p>
<p class="c94"> </p>
<p class="c95 textStyle15"> <a name="t113" id="t113"></a>Autism, DD-Emotional, ED</p>
<p class="c93 textStyle15">Pica</p>
<p class="c94"> </p>
<p class="c95 textStyle15">Autism, ED, MR/ID, OHI</p>
<p class="c93 textStyle15">Posttraumatic Stress Disorder</p>
<p class="c94"> </p>
<p class="c95 textStyle15"> <a name="t116" id="t116"></a>ED</p>
<p class="c93 textStyle15">Self-Injury</p>
<p class="c94"> </p>
<p class="c95 textStyle15">Autism, DD-Cognitive, DD-Emotional, ED, MR/ID</p>
<p class="c93 textStyle15">Specific Phobia</p>
<p class="c94"> </p>
<p class="c95 textStyle15">ED</p>
<p class="c93 textStyle15">Stealing</p>
<p class="c94"> </p>
<p class="c95 textStyle15"> <a name="t120" id="t120"></a>ED</p>
<p class="c93 textStyle15"> <a name="t121" id="t121"></a>Tics</p>
<p class="c94"> </p>
<p class="c95 textStyle15">OHI</p>
<p class="c93 textStyle15">Trichotillomania</p>
<p class="c94"> </p>
<p class="c95 textStyle15">ED</p>
<p class="c100 textStyle1">DD = Developmental Delay; ED = Emotional Disturbance; LD = Specific Learning Disability; MR/ID = Mental Retardation/Intellectual
Disability; OHI = Other Health Impairment; S/L = Speech or Language Impairment. </p>
<p class="c101"><span class="textStyle16">Note:</span><span class="textStyle1"> No scores were elevated: No need for follow-up indicated.</span></p>
<div class="c31"><img src="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0010_00.jpg" width="87.00pt" height="27.00pt" border="0" alt="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0010_00.jpg(347x108)"/></div>
<p class="c22 textStyle8">Page 10</p>
<p class="c11 textStyle8">Copyright © 2009 Multi-Health Systems Inc. All rights reserved.</p>
<p class="c23 textStyle4">ver. 1.2</p>
<span class="pageEnd" id="pge0010"> </span> <span class="pageStart" id="pgs0011"> </span>
<p class="c11 textStyle8">Conners Early Childhood–P Assessment Report for Thea Lacheta (101677)</p>
<p class="c12 textStyle8">Admin Date: 02/03/2022</p>
<p class="c33 textStyle3"> <a name="t123" id="t123"></a>Item Responses</p>
<p class="c15 textStyle2">The parent marked the following responses for items on the Conners Early Childhood–P.</p>
</div><div class="mlsection1"><h1 class="c102 textStyle5">Behavior Scales</h1>
<p class="c103 textStyle12">Item  <a name="t126" id="t126"></a>Parent's</p>
<p class="c104 textStyle12">Rating Item</p>
<p class="c105 textStyle12"> Parent's</p>
<p class="c106 textStyle12">Rating Item</p>
<p class="c107 textStyle12"> <a name="t129" id="t129"></a>Parent's
Rating</p>
<p class="c108 textStyle12"> <a name="t130" id="t130"></a>Item</p>
<p class="c109 textStyle12"> <a name="t131" id="t131"></a>Parent's
Rating</p>
<p class="c110 textStyle12"> <a name="t132" id="t132"></a>Item</p>
<p class="c111 textStyle12"> <a name="t133" id="t133"></a>Parent's
Rating</p>
<p class="c112 textStyle12"> <a name="t134" id="t134"></a>Item</p>
<p class="c113 textStyle12"> <a name="t135" id="t135"></a>Parent's
Rating</p>
<p class="c114 textStyle8">B1. 1 B20. 0 B39. 2 B58. 1 B77. 0 B96. 1
B2. 1 B21. 3 B40. 1 B59. 1 B78. 0 B97. 0
B3. 2 B22. 1 B41. 1 B60. 0 B79. 0 B98. 0
B4. 1 B23. 1 B42. 0 B61. 0 B80. 0 B99. 1
B5. 0 B24. 0 B43. 0 B62. 0 B81. 0 B100. 3
B6. 1 B25. 2 B44. 2 B63. 0 B82. 0 B101. 0
B7. 0 B26. 0 B45. 1 B64. 0 B83. 0 B102. 0
B8. 2 B27. 0 B46. 3 B65. 0 B84. 1 B103. 1
B9. 0 B28. 2 B47. 0 B66. 0 B85. 2 B104. 2
B10. 1 B29. 1 B48. 1 B67. 0 B86. 0 B105. 0
B11. 0 B30. 0 B49. 1 B68. 0 B87. 0 B106. 0
B12. 1 B31. 0 B50. 0 B69. 0 B88. 0 B107. 0
B13. 3 B32. 1 B51. 0 B70. 0 B89. 1 B108. 0
B14. 0 B33. 0 B52. 1 B71. 0 B90. 1 B109. 0
B15. 1 B34. 0 B53. 0 B72. 0 B91. 0 B110. 0
B16. 1 B35. 0 B54. 1 B73. 0 B92. 0</p>
<p class="c115 textStyle8">B17. 0 B36. 0 B55. 1 B74. 0 B93. 2</p>
<p class="c115 textStyle8">B18. 0 B37. 0 B56. 2 B75. 1 B94. 0</p>
<p class="c115 textStyle8">B19. 0 B38. 0 B57. 0 B76. 1 B95. 0</p>
<p class="c116"><span class="textStyle12">Parent’s rating: </span><span class="textStyle8"> 0 = Not true at all (Never, Seldom); 1 = Just a little true (Occasionally); 2 = Pretty much true (Often,
Quite a bit); 3 = Very much true (Very often, Very frequently); ? = Omitted Item.</span></p>
<p class="c117 textStyle12">Item Parent's<br/>Rating Item</p>
</div><div class="mlsection1"><h1 class="c102 textStyle5"> <a name="t140" id="t140"></a>Developmental Milestone Scales</h1>
<p class="c105 textStyle12"> <a name="t139" id="t139"></a>Parent's</p>
<p class="c106 textStyle12">Rating Item</p>
<p class="c107 textStyle12">Parent's
Rating</p>
<p class="c108 textStyle12"> <a name="t141" id="t141"></a>Item</p>
<p class="c109 textStyle12"> <a name="t142" id="t142"></a>Parent's
Rating</p>
<p class="c110 textStyle12"> <a name="t143" id="t143"></a>Item</p>
<p class="c111 textStyle12"> <a name="t144" id="t144"></a>Parent's
Rating</p>
<p class="c112 textStyle12"> <a name="t145" id="t145"></a>Item</p>
<p class="c113 textStyle12"> <a name="t146" id="t146"></a>Parent's
Rating</p>
<p class="c118"><span class="textStyle8">DM1. 1 DM14. 2 DM27. 2 DM40. 1 DM53. 2 DM66. 2
DM2. 2 DM15. 0 DM28. 1 DM41. 0 DM54. 0 DM67. 2
DM3. 2 DM16. 1 DM29. 2 DM42. 2 DM55. 2 DM68. 0
DM4. 0 DM17. 2 DM30. 2 DM43. 2 DM56. 0 DM69. 2
DM5. 2 DM18. 1 DM31. 0 DM44. 2 DM57. 1 DM70. 1
DM6. 1 DM19. 1 DM32. 2 DM45. 2 DM58. 2 DM71. 2
DM7. 2 DM20. 1 DM33. 1 DM46. 1 DM59. 1 DM72. 2
DM8. 1 DM21. 1 DM34. 2 DM47. 2 DM60. 0 DM73. 1
DM9. 0 DM22. 1 DM35. 2 DM48. 2 DM61. 1 DM74. 2
DM10. 0 DM23. 1 DM36. 2 DM49. 1 DM62. 2 DM75. 0
DM11. 2 DM24. 2 DM37. 0 DM50. 2 DM63. 1 IM1. 0
DM12. 0 DM25. 2 DM38. 2 DM51. 1 DM64. 2 IM2. 0
DM13. 1 DM26. 1 DM39. 2 DM52. 0 DM65. 0 IM3. 0
</span><span class="textStyle12">Parent’s rating (DM items):</span><span class="textStyle8"> 0 = No (Never or rarely); 1 = Sometimes; 2 = Yes (Always or almost always); ? = Omitted
item. </span></p>
<p class="c116"><span class="textStyle12">Parent’s rating (IM items):</span><span class="textStyle8"> 0 = Not true at all (Never, Seldom); 1 = Just a little true (Occasionally); 2 = Pretty much true
(Often, Quite a bit); 3 = Very much true (Very often, Very frequently); ? = Omitted item.</span></p>
<p class="c89 textStyle8"> <a name="t147" id="t147"></a>Date printed: February 03, 2022 </p>
<p class="c119 textStyle12">End of Report</p>
<p class="c11 textStyle8">Copyright © 2009 Multi-Health Systems Inc. All rights reserved.</p>
<div class="c31"><img src="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0011_00.jpg" width="87.00pt" height="27.00pt" border="0" alt="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0011_00.jpg(347x108)"/></div>
<p class="c22 textStyle8">Page 11</p>
<p class="c23 textStyle4">ver. 1.2</p>
<span class="pageEnd" id="pge0011"> </span> <span class="pageStart" id="pgs0012"> </span>
<p class="c11 textStyle8">Conners Early Childhood–P Assessment Report for Thea Lacheta (101677)</p>
<p class="c12 textStyle8">Admin Date: 02/03/2022</p>
<p class="c33 textStyle3"> <a name="t149" id="t149"></a>Items by Scale</p>
<p class="c18 textStyle2">This section of the report contains copyrighted items and information that is not intended for public
<a name="t150" id="t150"></a>disclosure. If it is necessary to provide a copy of this report to anyone other than the assessor, this section </p>
<p class="c16 textStyle2">must be removed. </p>
</div><div class="mlsection1"><h1 class="c102 textStyle5"> <a name="t152" id="t152"></a>Behavior Scales</h1>
<p class="c119 textStyle12">The following legend applies to the Behavior Scale items:</p>
<p class="c120 textStyle8">0 = Not true at all (Never, Seldom); 1 = Just a little true (Occasionally); 2 = Pretty much true (Often, Quite a bit); 3 =
Very much true (Very often, Very frequently); ? = Omitted Item. R = This item is reverse scored for score calculations.</p>
<p class="c121 textStyle7"> Inattention/Hyperactivity</p>
<p class="c122 textStyle7">Social Functioning/Atypical Behaviors</p>
<p class="c58 textStyle12">Item</p>
<p class="c123 textStyle12"> <a name="t156" id="t156"></a>Rating</p>
<p class="c124 textStyle12"> <a name="t157" id="t157"></a>Rating</p>
<p class="c125 textStyle1">B 12. Does not pay attention.</p>
<p class="c126 textStyle1">1</p>
<p class="c126 textStyle1">1</p>
<p class="c127 textStyle12"> <a name="t159" id="t159"></a>Item</p>
<p class="c128 textStyle13">Items not on subscale</p>
<p class="c125 textStyle1">B 22. Excitable, impulsive.</p>
<p class="c129 textStyle1"> <a name="t160" id="t160"></a>B 51. Is picked on by other children.</p>
<p class="c130 textStyle1">0</p>
<p class="c125 textStyle1">B 34. Has difficulty staying in seat.</p>
<p class="c126 textStyle1">0</p>
<p class="c126 textStyle1">0</p>
<p class="c128 textStyle13">Social Functioning subscale</p>
<p class="c125 textStyle1">B 42. Runs or climbs when supposed to walk or sit.</p>
<p class="c126 textStyle1">0</p>
<p class="c129 textStyle1">B 1. Makes friends easily. (R)</p>
<p class="c130 textStyle1">3</p>
<p class="c130 textStyle1">1</p>
<p class="c125 textStyle1">B 47. Acts as if driven by a motor.</p>
<p class="c126 textStyle1">1</p>
<p class="c129 textStyle1">B 13. Makes appropriate eye contact. (R)</p>
<p class="c130 textStyle1">3</p>
<p class="c125 textStyle1">B 49. Jumps from one activity to another.</p>
<p class="c126 textStyle1">1</p>
<p class="c129 textStyle1">B 21. Is appropriately affectionate. (R)</p>
<p class="c130 textStyle1">2</p>
<p class="c125 textStyle1">B 55. Is always “on the go.”</p>
<p class="c129 textStyle1">B 25. Gets invited to parties and play dates. (R)</p>
<p class="c130 textStyle1">2</p>
<p class="c131 textStyle1">B 62. Gets over-stimulated or “wound-up” in exciting
situations.</p>
<p class="c126 textStyle1">0</p>
<p class="c126 textStyle1">0</p>
<p class="c129 textStyle1">B 28. Smiles when other people smile at him/her. (R)</p>
<p class="c130 textStyle1">0</p>
<p class="c129 textStyle1">B 35. Is ignored by other children.</p>
<p class="c130 textStyle1">2</p>
<p class="c125 textStyle1">B 65. Acts before thinking.</p>
<p class="c129 textStyle1">B 44. Is liked by other children. (R)</p>
<p class="c130 textStyle1">3</p>
<p class="c125 textStyle1">B 72. Has a short attention span.</p>
<p class="c126 textStyle1">0</p>
<p class="c126 textStyle1">0</p>
<p class="c125 textStyle1">B 74. Fidgeting.</p>
<p class="c132 textStyle1">B 46. Is happy for others when something good
happens to them. (R)</p>
<p class="c130 textStyle1">0</p>
<p class="c125 textStyle1">B 76. Loses interest quickly.</p>
<p class="c126 textStyle1">1</p>
<p class="c129 textStyle1">B 53. Has trouble keeping friends.</p>
<p class="c130 textStyle1">2</p>
<p class="c125 textStyle1">B 79. Has difficulty focusing on just one thing.</p>
<p class="c126 textStyle1">0</p>
<p class="c126 textStyle1">0</p>
<p class="c129 textStyle1">B 56. Gets along well with other children. (R)</p>
<p class="c130 textStyle1">0</p>
<p class="c125 textStyle1">B 81. Restless or overactive.</p>
<p class="c126 textStyle1">0</p>
<p class="c129 textStyle1">B 61. Is rejected by peers.</p>
<p class="c125 textStyle1">B 86. Inattentive, easily distracted.</p>
<p class="c126 textStyle1">1</p>
<p class="c129 textStyle1">B 85. Tries to comfort others when they are upset. (R)</p>
<p class="c130 textStyle1">2</p>
<p class="c130 textStyle1">2</p>
<p class="c125 textStyle1">B 90. Fails to finish things he/she starts.</p>
<p class="c129 textStyle1"> <a name="t161" id="t161"></a>B 104. Wants to have friends. (R)</p>
<p class="c128 textStyle13">Atypical Behaviors subscale</p>
<p class="c121 textStyle7">Defiant/Aggressive Behaviors</p>
<p class="c129 textStyle1">B 5. Is odd or unusual.</p>
<p class="c130 textStyle1">0</p>
<p class="c133 textStyle12"> <a name="t164" id="t164"></a>Item</p>
<p class="c133 textStyle13">Defiance/Temper subscale</p>
<p class="c123 textStyle12"> <a name="t165" id="t165"></a>Rating</p>
<p class="c132 textStyle1">B 9. Makes sudden facial or body twitches (for example,
eye blinking, head jerking, or shoulder shrugging).</p>
<p class="c130 textStyle1">0</p>
<p class="c130 textStyle1">0</p>
<p class="c125 textStyle1">B 10. Sulks.</p>
<p class="c126 textStyle1">1</p>
<p class="c134 textStyle1">B 11. Eats non-food items (for example, wallpaper, dirt,
or garbage).</p>
<p class="c130 textStyle1">0</p>
<p class="c125 textStyle1">B 15. Gets mad easily.</p>
<p class="c134 textStyle1">B 14. Can’t seem to stop making repeated sounds (for
example, sniffing, throat clearing, or tongue clicking).</p>
<p class="c130 textStyle1">0</p>
<p class="c126 textStyle1">1</p>
<p class="c125 textStyle1">B 18. Is manipulative.</p>
<p class="c126 textStyle1">0</p>
<p class="c125 textStyle1">B 32. Whines and complains.</p>
<p class="c126 textStyle1">1</p>
<p class="c126 textStyle1">1</p>
<p class="c129 textStyle1">B 30. Has unusual interests.</p>
<p class="c125 textStyle1">B 45. Is stubborn.</p>
<p class="c125 textStyle1">B 48. Temper outbursts.</p>
<p class="c134 textStyle1">B 50. Repeats body movements over and over (for
example, rocking, spinning, or hand flapping).</p>
<p class="c130 textStyle1">1</p>
<p class="c130 textStyle1">0</p>
<p class="c126 textStyle1">1</p>
<p class="c125 textStyle1">B 64. Is defiant.</p>
<p class="c126 textStyle1">0</p>
<p class="c129 textStyle1">B 52. Prefers to be alone.</p>
<p class="c125 textStyle1">B 92. Is bossy.</p>
<p class="c126 textStyle1">0</p>
<p class="c129 textStyle1">B 63. Seems to be in his/her own world.</p>
<p class="c130 textStyle1">0</p>
<p class="c130 textStyle1">0</p>
<p class="c125 textStyle1">B 97. Argues with adults.</p>
<p class="c126 textStyle1">0</p>
<p class="c125 textStyle1">B 99. Refuses to do what he/she is asked to do.</p>
<p class="c126 textStyle1">0</p>
<p class="c126 textStyle1">1</p>
<p class="c134 textStyle1">B 68. Unusual use of language (for example, repeats
things, sounds like a robot or a little professor, uses a
high-pitched voice, uses made-up words).</p>
<p class="c130 textStyle1">1</p>
<p class="c125 textStyle1"> <a name="t166" id="t166"></a>B 101. Mood changes quickly and drastically.</p>
<p class="c133 textStyle13">Aggression subscale</p>
<p class="c129 textStyle1">B 75. Dislikes disruption to his/her routine.</p>
<p class="c130 textStyle1">0</p>
<p class="c125 textStyle1">B 37. Tries to hurt other people’s feelings.</p>
<p class="c126 textStyle1">0</p>
<p class="c126 textStyle1">0</p>
<p class="c129 textStyle1">B 88. Doesn’t show his/her emotions.</p>
<p class="c130 textStyle1">1</p>
<p class="c125 textStyle1">B 69. Lies to get things or to manipulate people.</p>
<p class="c126 textStyle1">0</p>
<p class="c129 textStyle1">B 89. Play is repetitive.</p>
<p class="c125 textStyle1">B 71. Threatens people.</p>
<p class="c134 textStyle1">B 110. Hurts self (for example, cuts self, picks at skin,
or bangs head).</p>
<p class="c130 textStyle1">0</p>
<p class="c125 textStyle1">B 77. Swears or uses bad language.</p>
<p class="c126 textStyle1">0</p>
<p class="c126 textStyle1">0</p>
<p class="c125 textStyle1">B 91. Picks on other children.</p>
<p class="c126 textStyle1">0</p>
<p class="c125 textStyle1">B 95. Is rude.</p>
<p class="c125 textStyle1">B 106. Gets into fights.</p>
<p class="c126 textStyle1">0</p>
<p class="c125 textStyle1">B 108. Destroys things on purpose.</p>
<p class="c126 textStyle1">0</p>
<div class="c31"><img src="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0012_00.jpg" width="87.00pt" height="27.00pt" border="0" alt="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0012_00.jpg(347x108)"/></div>
<p class="c11 textStyle8">Copyright © 2009 Multi-Health Systems Inc. All rights reserved.</p>
<p class="c22 textStyle8">Page 12</p>
<p class="c23 textStyle4">ver. 1.2</p>
<span class="pageEnd" id="pge0012"> </span> <span class="pageStart" id="pgs0013"> </span>
<p class="c11 textStyle8">Conners Early Childhood–P Assessment Report for Thea Lacheta (101677)</p>
<p class="c12 textStyle8">Admin Date: 02/03/2022</p>
<p class="c121 textStyle7"> <a name="t169" id="t169"></a>Anxiety</p>
<p class="c135 textStyle9"> <a name="t168" id="t168"></a>Conners Early Childhood Global Index</p>
<p class="c58 textStyle12">Item</p>
<p class="c123 textStyle12"> <a name="t170" id="t170"></a>Rating</p>
<p class="c125 textStyle1">B 2. Has trouble controlling his/her worries.</p>
<p class="c126 textStyle1">1</p>
<p class="c126 textStyle1">1</p>
<p class="c125 textStyle1">B 4. Worries.</p>
<p class="c127 textStyle12"> <a name="t172" id="t172"></a>Item</p>
<p class="c128 textStyle13">Restless-Impulsive subscale</p>
<p class="c124 textStyle12"> <a name="t173" id="t173"></a>Rating</p>
<p class="c125 textStyle1">B 24. Has trouble falling asleep when alone.</p>
<p class="c126 textStyle1">1</p>
<p class="c126 textStyle1">0</p>
<p class="c125 textStyle1">B 29. Is overly clingy or attached to parent(s).</p>
<p class="c126 textStyle1">0</p>
<p class="c134 textStyle1">B 8. Demands must be met immediately – easily
frustrated.</p>
<p class="c130 textStyle1">1</p>
<p class="c130 textStyle1">2</p>
<p class="c131 textStyle1">B 33. Wakes up during the night, then has trouble
falling back to sleep.</p>
<p class="c126 textStyle1">0</p>
<p class="c129 textStyle1">B 22. Excitable, impulsive.</p>
<p class="c125 textStyle1">B 38. Anticipates the worst.</p>
<p class="c129 textStyle1">B 74. Fidgeting.</p>
<p class="c130 textStyle1">0</p>
<p class="c125 textStyle1">B 40. Has nightmares or night terrors.</p>
<p class="c126 textStyle1">1</p>
<p class="c126 textStyle1">1</p>
<p class="c129 textStyle1">B 81. Restless or overactive.</p>
<p class="c130 textStyle1">0</p>
<p class="c130 textStyle1">0</p>
<p class="c129 textStyle1">B 86. Inattentive, easily distracted.</p>
<p class="c130 textStyle1">1</p>
<p class="c131 textStyle1">B 41. Is afraid of one or more specific objects or
situations (for example, animals, insects, blood,
                doctors, water, storms, heights, or places).</p>
<p class="c126 textStyle1">0</p>
<p class="c129 textStyle1">B 90. Fails to finish things he/she starts.</p>
<p class="c130 textStyle1">0</p>
<p class="c129 textStyle1"> <a name="t174" id="t174"></a>B 107. Disturbs other children.</p>
<p class="c125 textStyle1">B 43. Appears “on edge,” nervous, or jumpy.</p>
<p class="c126 textStyle1">1</p>
<p class="c128 textStyle13">Emotional Lability subscale</p>
<p class="c125 textStyle1">B 58. Is timid, easily frightened.</p>
<p class="c126 textStyle1">1</p>
<p class="c129 textStyle1">B 48. Temper outbursts.</p>
<p class="c130 textStyle1">1</p>
<p class="c125 textStyle1">B 59. Feelings are easily hurt.</p>
<p class="c126 textStyle1">0</p>
<p class="c129 textStyle1">B 101. Mood changes quickly and drastically.</p>
<p class="c130 textStyle1">0</p>
<p class="c130 textStyle1">0</p>
<p class="c125 textStyle1">B 66. Is afraid to be alone.</p>
<p class="c129 textStyle1">B 109. Cries often and easily.</p>
<p class="c125 textStyle1">B 80. Has trouble falling asleep.</p>
<p class="c126 textStyle1">0</p>
<p class="c126 textStyle1">0</p>
<p class="c125 textStyle1">B 82. Complains about aches and pains.</p>
<p class="c126 textStyle1">0</p>
<p class="c125 textStyle1">B 98. Is anxious.</p>
<p class="c135 textStyle9"> <a name="t175" id="t175"></a>Other Clinical Indicators</p>
<p class="c125 textStyle1">B 109. Cries often and easily.</p>
<p class="c126 textStyle1">0</p>
<p class="c128 textStyle12"> <a name="t176" id="t176"></a>Item</p>
<p class="c124 textStyle12"> <a name="t177" id="t177"></a>Rating</p>
<p class="c121 textStyle7"> <a name="t179" id="t179"></a>Mood and Affect</p>
<p class="c129 textStyle1">B 7. Is cruel to animals.</p>
<p class="c130 textStyle1">0</p>
<p class="c133 textStyle12">Item</p>
<p class="c123 textStyle12"> <a name="t180" id="t180"></a>Rating</p>
<p class="c134 textStyle1">B 9. Makes sudden facial or body twitches (for example,
eye blinking, head jerking, or shoulder shrugging).</p>
<p class="c130 textStyle1">0</p>
<p class="c130 textStyle1">0</p>
<p class="c125 textStyle1">B 6. Is irritated easily.</p>
<p class="c126 textStyle1">1</p>
<p class="c125 textStyle1">B 10. Sulks.</p>
<p class="c126 textStyle1">1</p>
<p class="c132 textStyle1">B 11. Eats non-food items (for example, wallpaper, dirt,
or garbage).</p>
<p class="c130 textStyle1">0</p>
<p class="c125 textStyle1">B 15. Gets mad easily.</p>
<p class="c126 textStyle1">1</p>
<p class="c125 textStyle1">B 26. Play involves sad or morbid themes.</p>
<p class="c126 textStyle1">0</p>
<p class="c126 textStyle1">0</p>
<p class="c134 textStyle1">B 14. Can’t seem to stop making repeated sounds (for
example, sniffing, throat clearing, or tongue clicking).</p>
<p class="c130 textStyle1">1</p>
<p class="c131 textStyle1">B 36. Says bad things about self (for example, “I’m
dumb,” or, “I can’t do anything right”).</p>
<p class="c126 textStyle1">1</p>
<p class="c132 textStyle1">B 41. Is afraid of one or more specific objects or
situations (for example, animals, insects, blood,
                doctors, water, storms, heights, or places).</p>
<p class="c130 textStyle1">0</p>
<p class="c125 textStyle1">B 48. Temper outbursts.</p>
<p class="c125 textStyle1">B 52. Prefers to be alone.</p>
<p class="c126 textStyle1">1</p>
<p class="c129 textStyle1">B 57. Steals.</p>
<p class="c125 textStyle1">B 70. Seems sad.</p>
<p class="c126 textStyle1">0</p>
<p class="c129 textStyle1">B 73. Sets fires or plays with matches.</p>
<p class="c130 textStyle1">0</p>
<p class="c130 textStyle1">0</p>
<p class="c125 textStyle1">B 83. Does not enjoy things.</p>
<p class="c126 textStyle1">0</p>
<p class="c134 textStyle1">B 94. Has been exposed to a serious accident, extreme
violence, trauma, abuse, or neglect.</p>
<p class="c130 textStyle1">0</p>
<p class="c125 textStyle1">B 101. Mood changes quickly and drastically.</p>
<p class="c126 textStyle1">0</p>
<p class="c126 textStyle1">0</p>
<p class="c125 textStyle1">B 109. Cries often and easily.</p>
<p class="c129 textStyle1">B 102. Is a perfectionist.</p>
<p class="c130 textStyle1">0</p>
<p class="c136 textStyle1">B 105. Pulls out hair from his/her scalp, eyelashes, or
other places, to the point that you can notice bald
patches.</p>
<p class="c130 textStyle1">0</p>
<p class="c121 textStyle7">Physical Symptoms</p>
<p class="c133 textStyle12"> <a name="t183" id="t183"></a>Item</p>
<p class="c133 textStyle13">Items not on subscale</p>
<p class="c123 textStyle12"> <a name="t184" id="t184"></a>Rating</p>
<p class="c132 textStyle1">B 110. Hurts self (for example, cuts self, picks at skin,
or bangs head).</p>
<p class="c125 textStyle1">B 17. Eats too much.</p>
<p class="c126 textStyle1">0</p>
<p class="c125 textStyle1">B 19. Eats too little.</p>
<p class="c126 textStyle1">0</p>
<p class="c125 textStyle1">B 20. Complains about stomach aches.</p>
<p class="c126 textStyle1">0</p>
<p class="c126 textStyle1">0</p>
<p class="c131 textStyle1">B 27. Complains about being sick, even when nothing is
wrong.</p>
<p class="c126 textStyle1">0</p>
<p class="c125 textStyle1">B 82. Complains about aches and pains.</p>
<p class="c126 textStyle1">0</p>
<p class="c125 textStyle1"> <a name="t185" id="t185"></a>B 87. Complains about headaches.</p>
<p class="c133 textStyle13">Sleep Problems</p>
<p class="c125 textStyle1">B 31. Sleeps too much.</p>
<p class="c126 textStyle1">0</p>
<p class="c137 textStyle1">B 33. Wakes up during the night, then has trouble
falling back to sleep.</p>
<p class="c126 textStyle1">1</p>
<p class="c126 textStyle1">0</p>
<p class="c125 textStyle1">B 40. Has nightmares or night terrors.</p>
<p class="c126 textStyle1">0</p>
<p class="c125 textStyle1">B 80. Has trouble falling asleep.</p>
<p class="c126 textStyle1">1</p>
<p class="c125 textStyle1">B 84. Wakes up too early.</p>
<div class="c31"><img src="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0013_00.jpg" width="87.00pt" height="27.00pt" border="0" alt="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0013_00.jpg(347x108)"/></div>
<p class="c11 textStyle8">Copyright © 2009 Multi-Health Systems Inc. All rights reserved.</p>
<p class="c22 textStyle8">Page 13</p>
<p class="c23 textStyle4">ver. 1.2</p>
<span class="pageEnd" id="pge0013"> </span> <span class="pageStart" id="pgs0014"> </span>
<p class="c11 textStyle8">Conners Early Childhood–P Assessment Report for Thea Lacheta (101677)</p>
<p class="c12 textStyle8">Admin Date: 02/03/2022</p>
</div><div class="mlsection1"><h1 class="c14 textStyle5"> <a name="t187" id="t187"></a>Developmental Milestone Scales</h1>
<p class="c119 textStyle12">The following legend applies to the Developmental Milestone Scale items:</p>
<p class="c120 textStyle8"> 0 = No (Never or rarely); 1 = Sometimes; 2 = Yes (Always or almost always); ? = Omitted Item. R = This item is reverse
scored for score calculations.</p>
<p class="c138 textStyle7"> <a name="t190" id="t190"></a>Adaptive Skills</p>
<p class="c139 textStyle7"> <a name="t192" id="t192"></a>Communication</p>
<p class="c140"><span class="textStyle12">Item
</span><span class="textStyle13">Dressing</span></p>
<p class="c141 textStyle12"> <a name="t191" id="t191"></a>Rating</p>
<p class="c142"><span class="textStyle12">Item
</span><span class="textStyle13">Expressive</span></p>
<p class="c143 textStyle12"> <a name="t193" id="t193"></a>Rating</p>
<p class="c144 textStyle1">DM 13. Puts his/her shoes on correct feet. (R)</p>
<p class="c145 textStyle1">1</p>
<p class="c145 textStyle1">1</p>
<p class="c146 textStyle1">2</p>
<p class="c144 textStyle1">DM 20. Removes a front-opening jacket. (R)</p>
<p class="c145 textStyle1">1</p>
<p class="c147 textStyle1">DM 3. Communicates wants and needs using either
words or gestures. (R)</p>
<p class="c146 textStyle1">1</p>
<p class="c144 textStyle1">DM 22. Puts on a front-opening jacket. (R)</p>
<p class="c145 textStyle1">2</p>
<p class="c148 textStyle1">DM 8. Uses plural words (for example, one block; two
blocks). (R)</p>
<p class="c146 textStyle1">2</p>
<p class="c149 textStyle1">DM 29. Pulls up pants with an elastic waistband (for
example, sweat pants). (R)</p>
<p class="c145 textStyle1">0</p>
<p class="c148 textStyle1">DM 17. Uses “I” or “my” (instead of using his/her name)
to talk about him/herself. (R)</p>
<p class="c146 textStyle1">1</p>
<p class="c144 textStyle1">DM 41. Puts on his/her own sweatshirt or t-shirt. (R)</p>
<p class="c145 textStyle1">0</p>
<p class="c144 textStyle1"> <a name="t194" id="t194"></a>DM 54. Completely dresses him/herself. (R)</p>
<p class="c148 textStyle1">DM 23. Uses gestures like pointing to things, nodding
“yes,” and waving good-bye. (R)</p>
<p class="c146 textStyle1">1</p>
<p class="c150 textStyle13">Eating/Drinking</p>
<p class="c144 textStyle1">DM 35. Feeds self with a spoon without spilling. (R)</p>
<p class="c145 textStyle1">2</p>
<p class="c145 textStyle1">2</p>
<p class="c151 textStyle1">DM 28. Expresses ideas in more than one way (if you
don’t understand, he/she can say it using different
words). (R)</p>
<p class="c146 textStyle1">0</p>
<p class="c144 textStyle1">DM 39. Drinks from a cup without spilling. (R)</p>
<p class="c145 textStyle1">0</p>
<p class="c152 textStyle1">DM 52. Uses a knife for spreading and cutting soft
<a name="t195" id="t195"></a>foods (for example, a jelly sandwich). (R)</p>
<p class="c148 textStyle1">DM 31. Changes verbs to talk about the past or future
(for example, I walked, I will walk). (R)</p>
<p class="c146 textStyle1">2</p>
<p class="c150 textStyle13">Toileting</p>
<p class="c153 textStyle1">DM 34. Uses the words “a,” “an,” and “the.” (R)</p>
<p class="c146 textStyle1">2</p>
<p class="c149 textStyle1">DM 1. Knows when he/she needs to go to the
bathroom. (R)</p>
<p class="c145 textStyle1">0</p>
<p class="c145 textStyle1">1</p>
<p class="c148 textStyle1">DM 38. Labels his/her emotions (for example, happy or
sad). (R)</p>
<p class="c146 textStyle1">2</p>
<p class="c144 textStyle1"> <a name="t196" id="t196"></a>DM 15. Is fully toilet-trained (day and night). (R)</p>
<p class="c147 textStyle1">DM 42. Spontaneously names things he/she sees in the
world. (R)</p>
<p class="c146 textStyle1">1</p>
<p class="c150 textStyle13">Hygiene</p>
<p class="c149 textStyle1">DM 9. Bathes him/herself (soaps, shampoos, and
rinses). (R)</p>
<p class="c145 textStyle1">1</p>
<p class="c145 textStyle1">0</p>
<p class="c153 textStyle1">DM 49. Combines words into phrases. (R)</p>
<p class="c146 textStyle1">2</p>
<p class="c153 textStyle1">DM 64. Shows emotions with facial expressions. (R)</p>
<p class="c146 textStyle1">1</p>
<p class="c144 textStyle1">DM 18. Brushes hair. (R)</p>
<p class="c153 textStyle1"> <a name="t197" id="t197"></a>DM 70. Talks in complete sentences. (R)</p>
<p class="c144 textStyle1">DM 48. Wipes nose with tissue. (R)</p>
<p class="c145 textStyle1">2</p>
<p class="c145 textStyle1">2</p>
<p class="c154 textStyle13">Receptive</p>
<p class="c144 textStyle1"> <a name="t198" id="t198"></a>DM 50. Brushes teeth. (R)</p>
<p class="c147 textStyle1">DM 11. Recognizes different emotions in others from
their facial expressions. (R)</p>
<p class="c146 textStyle1">2</p>
<p class="c146 textStyle1">2</p>
<p class="c150 textStyle13">Helping</p>
<p class="c149 textStyle1">DM 6. Completes household chores (for example, sets
the table or takes out the trash). (R)</p>
<p class="c145 textStyle1">1</p>
<p class="c145 textStyle1">1</p>
<p class="c147 textStyle1">DM 25. Explains answers to simple problems (for
example, “What do you do if you are hungry?”). (R)</p>
<p class="c146 textStyle1">2</p>
<p class="c144 textStyle1">DM 46. Helps clean up toys or belongings. (R)</p>
<p class="c147 textStyle1">DM 36. Follows one-step directions (for example, “Put
that down”). (R)</p>
<p class="c146 textStyle1">2</p>
<p class="c148 textStyle1">DM 62. Understands the meaning of common gestures
(for example, looks at an object when you point to it).</p>
<p class="c153 textStyle1"><br/>
(R)</p>
<p class="c146 textStyle1">2</p>
<p class="c147 textStyle1">DM 66. Follows directions that use the words “up,”
“down,” “over,” and “under.” (R)</p>
<p class="c11 textStyle8">Copyright © 2009 Multi-Health Systems Inc. All rights reserved.</p>
<div class="c31"><img src="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0014_00.jpg" width="87.00pt" height="27.00pt" border="0" alt="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0014_00.jpg(347x108)"/></div>
<p class="c22 textStyle8">Page 14</p>
<p class="c23 textStyle4">ver. 1.2</p>
<span class="pageEnd" id="pge0014"> </span> <span class="pageStart" id="pgs0015"> </span>
<p class="c11 textStyle8">Conners Early Childhood–P Assessment Report for Thea Lacheta (101677)</p>
<p class="c12 textStyle8">Admin Date: 02/03/2022</p>
<p class="c121 textStyle7"> <a name="t201" id="t201"></a>Motor Skills</p>
<p class="c122 textStyle7"> <a name="t203" id="t203"></a>Pre-Academic/Cognitive</p>
<p class="c155"><span class="textStyle12">Item
</span><span class="textStyle13">Fine</span></p>
<p class="c123 textStyle12"> <a name="t202" id="t202"></a>Rating</p>
<p class="c127 textStyle12">Item</p>
<p class="c124 textStyle12"> <a name="t204" id="t204"></a>Rating</p>
<p class="c129 textStyle1">DM 2. Counts from 1 to 5 without making mistakes. (R)</p>
<p class="c130 textStyle1">2</p>
<p class="c130 textStyle1">2</p>
<p class="c125 textStyle1">DM 10. Ties his/her shoelaces. (R)</p>
<p class="c126 textStyle1">1</p>
<p class="c126 textStyle1">0</p>
<p class="c129 textStyle1">DM 5. Says or sings the alphabet without mistakes. (R)</p>
<p class="c130 textStyle1">2</p>
<p class="c131 textStyle1">DM 21. Draws recognizable shapes (for example, lines,
circles, triangles, or squares). (R)</p>
<p class="c126 textStyle1">2</p>
<p class="c134 textStyle1">DM 7. Knows basic colors (including red, orange,
yellow, green, blue, and purple). (R)</p>
<p class="c130 textStyle1">0</p>
<p class="c137 textStyle1">DM 30. Handles small objects (for example, can string
beads or remove the wrapper from a piece of gum). (R)</p>
<p class="c126 textStyle1">2</p>
<p class="c132 textStyle1">DM 12. Rhymes words (for example, if I say the word
"cat," he/she can reply with "mat" or "hat"). (R)</p>
<p class="c130 textStyle1">1</p>
<p class="c125 textStyle1">DM 32. Cuts with scissors. (R)</p>
<p class="c126 textStyle1">2</p>
<p class="c132 textStyle1">DM 16. Knows basic shapes (including circle, square,
and triangle). (R)</p>
<p class="c130 textStyle1">1</p>
<p class="c125 textStyle1">DM 45. Drinks through a straw. (R)</p>
<p class="c126 textStyle1">1</p>
<p class="c131 textStyle1">DM 57. Fastens his/her clothing (for example, buttons,
snaps, or zippers). (R)</p>
<p class="c126 textStyle1">0</p>
<p class="c129 textStyle1">DM 19. Follows (“reads”) along with familiar books. (R)</p>
<p class="c130 textStyle1">1</p>
<p class="c132 textStyle1">DM 26. Understands words about time of day (for
example, “yesterday,” “dinnertime,” and “morning”). (R)</p>
<p class="c130 textStyle1">1</p>
<p class="c125 textStyle1">DM 60. Colors within the lines. (R)</p>
<p class="c126 textStyle1">0</p>
<p class="c125 textStyle1"> <a name="t205" id="t205"></a>DM 75. Cuts neatly around a shape. (R)</p>
<p class="c132 textStyle1">DM 33. Compares objects using the concepts
heavier/lighter and bigger/smaller. (R)</p>
<p class="c130 textStyle1">0</p>
<p class="c133 textStyle13">Gross</p>
<p class="c125 textStyle1">DM 4. Rides a bicycle without training wheels. (R)</p>
<p class="c126 textStyle1">2</p>
<p class="c126 textStyle1">0</p>
<p class="c132 textStyle1">DM 37. Names opposites (for example, hot/cold,
boy/girl, light/dark, big/small). (R)</p>
<p class="c130 textStyle1">1</p>
<p class="c125 textStyle1">DM 14. Swings on a swing. (R)</p>
<p class="c126 textStyle1">2</p>
<p class="c137 textStyle1">DM 24. Alternates feet when climbing stairs (one foot
per step). (R)</p>
<p class="c126 textStyle1">2</p>
<p class="c134 textStyle1">DM 40. Points to the correct letter or number when you
say its name. (R)</p>
<p class="c130 textStyle1">2</p>
<p class="c125 textStyle1">DM 27. Kicks a ball. (R)</p>
<p class="c134 textStyle1">DM 44. Answers questions about a picture story (for
example, “Where did Peter go?”). (R)</p>
<p class="c130 textStyle1">2</p>
<p class="c137 textStyle1">DM 43. Catches a small ball (for example, a baseball or
a tennis ball) that is thrown to him/her. (R)</p>
<p class="c126 textStyle1">1</p>
<p class="c126 textStyle1">2</p>
<p class="c134 textStyle1">DM 47. Groups or matches similar objects (for example,
things that are the same color). (R)</p>
<p class="c130 textStyle1">2</p>
<p class="c137 textStyle1">DM 51. Catches a big ball (for example, a basketball)
that is bounced to him/her. (R)</p>
<p class="c126 textStyle1">2</p>
<p class="c132 textStyle1">DM 53. Names most body parts (including shoulder,
elbow, wrist, and knee). (R)</p>
<p class="c130 textStyle1">0</p>
<p class="c125 textStyle1">DM 67. Hops on one foot. (R)</p>
<p class="c126 textStyle1">2</p>
<p class="c129 textStyle1">DM 56. Tells you his/her first and last name. (R)</p>
<p class="c130 textStyle1">1</p>
<p class="c125 textStyle1">DM 71. Jumps up and down. (R)</p>
<p class="c126 textStyle1">1</p>
<p class="c129 textStyle1">DM 59. Accurately counts at least 10 objects. (R)</p>
<p class="c130 textStyle1">1</p>
<p class="c131 textStyle1">DM 73. Stands on one foot for at least 5 seconds
without holding on to anything. (R)</p>
<p class="c134 textStyle1">DM 61. Understands the concepts “more,” “less,” and
“same.” (R)</p>
<p class="c130 textStyle1">1</p>
<p class="c129 textStyle1">DM 63. Identifies all 26 letters of the alphabet. (R)</p>
<p class="c130 textStyle1">0</p>
<p class="c121 textStyle7"> <a name="t207" id="t207"></a>Play</p>
<p class="c129 textStyle1">DM 65. Prints his/her first name (without copying). (R)</p>
<p class="c130 textStyle1">0</p>
<p class="c133 textStyle12">Item</p>
<p class="c123 textStyle12"> <a name="t208" id="t208"></a>Rating</p>
<p class="c134 textStyle1">DM 68. Draws a person with a body, arms, legs, hands,
feet, eyes, nose, and mouth. (R)</p>
<p class="c125 textStyle1">DM 55. Uses imagination in play. (R)</p>
<p class="c126 textStyle1">2</p>
<p class="c126 textStyle1">2</p>
<p class="c156 textStyle1">DM 58. Uses objects to represent different things (for
example, a block might be a car, a building, a
spaceship, or a hamburger). (R)</p>
<p class="c126 textStyle1">2</p>
<p class="c137 textStyle1">DM 69. Acts out different imaginative stories that he/she
has made up (not just imitating a television show). (R)</p>
<p class="c126 textStyle1">2</p>
<p class="c137 textStyle1">DM 72. Pretends to play different characters with action
figures, dolls, or people. (R)</p>
<p class="c126 textStyle1">2</p>
<p class="c137 textStyle1">DM 74. Pretends to do things that adults do (for
example, pretends to be a teacher or a doctor, plays
dress-up). (R)</p>
<p class="c11 textStyle8">Copyright © 2009 Multi-Health Systems Inc. All rights reserved.</p>
<div class="c31"><img src="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0015_00.jpg" width="87.00pt" height="27.00pt" border="0" alt="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0015_00.jpg(347x108)"/></div>
<p class="c22 textStyle8">Page 15</p>
<p class="c23 textStyle4">ver. 1.2</p>
<span class="pageEnd" id="pge0015"> </span> <span class="pageStart" id="pgs0016"> </span>
<p class="c11 textStyle8">Conners Early Childhood–P Assessment Report for Thea Lacheta (101677)</p>
<p class="c12 textStyle8">Admin Date: 02/03/2022</p>
<p class="c119 textStyle12"> <a name="t209" id="t209"></a>The following legend applies to the Impairment and Validity Scale items:</p>
<p class="c120 textStyle8">0 = Not true at all (Never, Seldom); 1 = Just a little true (Occasionally); 2 = Pretty much true (Often, Quite a bit); 3 =
Very much true (Very often, Very frequently); ? = Omitted Item. R = This item is reverse scored for score calculations.</p>
</div><div class="mlsection1"><h1 class="c157 textStyle5"> Impairment</h1>
<p class="c42 textStyle12">Item</p>
<p class="c158 textStyle12"> <a name="t212" id="t212"></a>Rating</p>
<p class="c159 textStyle1">IM 1. Your child’s problems seriously affect learning.</p>
<p class="c160 textStyle1">0</p>
<p class="c160 textStyle1">0</p>
<p class="c159 textStyle1">IM 2. Your child’s problems seriously affect interactions with other children.</p>
<p class="c160 textStyle1">0</p>
<p class="c159 textStyle1">IM 3. Your child's problems seriously affect home life.</p>
<p class="c11"> </p>
</div><div class="mlsection1"><h1 class="c14 textStyle5">Validity Scales</h1>
<p class="c73 textStyle7"> <a name="t215" id="t215"></a>Positive Impression</p>
<p class="c42 textStyle12">Item</p>
<p class="c158 textStyle12"> <a name="t216" id="t216"></a>Rating</p>
<p class="c159 textStyle1">B 3. Behaves like an angel.</p>
<p class="c160 textStyle1">2</p>
<p class="c159 textStyle1">B 39. Tells the truth; does not even tell “little white lies.”</p>
<p class="c160 textStyle1">1</p>
<p class="c160 textStyle1">2</p>
<p class="c159 textStyle1">B 54. Makes mistakes. (R)</p>
<p class="c159 textStyle1">B 78. Is perfect in every way.</p>
<p class="c160 textStyle1">0</p>
<p class="c159 textStyle1">B 96. Has to struggle to complete hard tasks. (R)</p>
<p class="c160 textStyle1">1</p>
<p class="c160 textStyle1">1</p>
<p class="c159 textStyle1">B 103. Is patient and content, even when waiting in a long line.</p>
<p class="c11"> </p>
<p class="c161 textStyle7"> <a name="t218" id="t218"></a>Negative Impression</p>
<p class="c42 textStyle12">Item</p>
<p class="c158 textStyle12"> <a name="t219" id="t219"></a>Rating</p>
<p class="c159 textStyle1">B 16. Is difficult to please or amuse.</p>
<p class="c160 textStyle1">1</p>
<p class="c159 textStyle1">B 23. I cannot figure out what makes him/her happy.</p>
<p class="c160 textStyle1">0</p>
<p class="c160 textStyle1">1</p>
<p class="c159 textStyle1">B 60. Cannot do things right.</p>
<p class="c159 textStyle1">B 67. Is hard to motivate (even with rewards like candy or toys).</p>
<p class="c160 textStyle1">2</p>
<p class="c160 textStyle1">0</p>
<p class="c159 textStyle1">B 93. Is fun to be around. (R)</p>
<p class="c159 textStyle1">B 100. Is happy, cheerful, and has a positive attitude. (R)</p>
<p class="c11"> </p>
<p class="c160 textStyle1">3</p>
<p class="c73 textStyle7"> <a name="t221" id="t221"></a>Inconsistency Index</p>
<p class="c42 textStyle12">Item</p>
<p class="c162 textStyle12"> <a name="t222" id="t222"></a>Rating</p>
<p class="c163 textStyle12"> <a name="t223" id="t223"></a>Item</p>
<p class="c164 textStyle12"> <a name="t224" id="t224"></a>Rating</p>
<p class="c159 textStyle1">B 72. Has a short attention span.</p>
<p class="c165 textStyle1">0</p>
<p class="c166 textStyle1">B 86. Inattentive, easily distracted. 0</p>
<p class="c165 textStyle1">0</p>
<p class="c159 textStyle1">B 53. Has trouble keeping friends.</p>
<p class="c166 textStyle1">B 61. Is rejected by peers.</p>
<p class="c167 textStyle1">0</p>
<p class="c159 textStyle1">B 2. Has trouble controlling his/her worries.</p>
<p class="c166 textStyle1">B 4. Worries.</p>
<p class="c165 textStyle1">1</p>
<p class="c167 textStyle1">1</p>
<p class="c159 textStyle1">B 76. Loses interest quickly.</p>
<p class="c165 textStyle1">1</p>
<p class="c168 textStyle1">B 79. Has difficulty focusing on just one
thing.</p>
<p class="c167 textStyle1">0</p>
<p class="c159 textStyle1">B 44. Is liked by other children. (R)</p>
<p class="c166 textStyle1">B 56. Gets along well with other children.</p>
<p class="c166 textStyle1"><br/>
(R)</p>
<p class="c165 textStyle1">2</p>
<p class="c167 textStyle1">2</p>
<p class="c159 textStyle1">B 34. Has difficulty staying in seat.</p>
<p class="c166 textStyle1">B 81. Restless or overactive.</p>
<p class="c165 textStyle1">0</p>
<p class="c167 textStyle1">0</p>
<p class="c159 textStyle1">B 15. Gets mad easily.</p>
<p class="c165 textStyle1">1</p>
<p class="c166 textStyle1">B 48. Temper outbursts.</p>
<p class="c167 textStyle1">1</p>
<p class="c159 textStyle1">B 6. Is irritated easily.</p>
<p class="c165 textStyle1">1</p>
<p class="c168 textStyle1">B 8. Demands must be met immediately –
easily frustrated.</p>
<p class="c167 textStyle1">2</p>
<p class="c159 textStyle1">B 37. Tries to hurt other people’s feelings.</p>
<p class="c166 textStyle1">B 71. Threatens people.</p>
<p class="c165 textStyle1">0</p>
<p class="c167 textStyle1">0</p>
<p class="c159 textStyle1">B 52. Prefers to be alone.</p>
<p class="c165 textStyle1">1</p>
<p class="c166 textStyle1">B 63. Seems to be in his/her own world. 0</p>
<p class="c11 textStyle8">Copyright © 2009 Multi-Health Systems Inc. All rights reserved.</p>
<div class="c31"><img src="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0016_00.jpg" width="87.00pt" height="27.00pt" border="0" alt="Thea_Lacheta_101677_ConnersEC_AssessmentReport_pdf_parts/0016_00.jpg(347x108)"/></div>
<p class="c22 textStyle8">Page 16</p>
<p class="c23 textStyle4">ver. 1.2</p>
<span class="pageEnd" id="pge0016"> </span>
</div></body>
</html>"""))