MYFILES ARE IN GIVEN FOLDER:
cs457-cs557-pa1-sdesai8/Fall_2018/DS/ on github.



No need to compile:

In some case use:
    "chmod +x server.py"

To run on remote machine:(WITH EXAMPLE)
    1. First run server by ./server
      ./MyWebServer.py
      (' ------------SERVER STARTED ON ', 'remote01', ':', '128.226.114.201', 52207, '-----------')

    2. On Testing folder use wget
          wget http://remote01.cs.binghamton.edu:52207/pdf-sample.pdf

    3. To check validity of server use "diff"
          diff test/pdf-sample.pdf cs457-cs557-pa1-sdesai8/Fall_2018/DS/www/pdf-sample.pdf
