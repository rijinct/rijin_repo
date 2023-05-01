from PIL import ImageGrab
from datetime import datetime

while True:
    im = ImageGrab.grab()
    dt = datetime.now()
    fname = "pic_{}.{}.png".format(dt.strftime("%H%M_%S"), dt.microsecond // 100000)
    file_loc = r"C:\Users\rijin.thomas\OneDrive - Gold Corporation\Desktop\screenshots"
    abs_fname = '{d}\{f}'.format(d=file_loc,f=fname)
    im.save(abs_fname, 'png') 