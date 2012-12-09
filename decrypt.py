import base64
import hmac,hashlib

price = "UAPCmgAKaKoKg3oFwpotbRYhZC41KN53eOXCgQ"
price = price.replace("-","+").replace("_","/")
price = price + '=' * (4 - len(price) % 4)
dprice=base64.b64decode(price)
initvec=dprice[0:16]
ciphertext=dprice[16:24]
integritysig=dprice[24:28]
ekey=base64.b64decode("SwkocWk+H59O8rf3uVAUMXLUfGn6rWiPX/Ua1pXMh/8=")
ikey=base64.b64decode("sH7xBkxKKqtQ3lVTpPT/Z8sBqUJAymjCkMA3JGa9lfU=")
pad = hmac.new(ekey, initvec, hashlib.sha1).digest()
l = [bin(ord(a) ^ ord(b)) for a,b in zip(ciphertext,pad)]
k = int("".join("%02x" % int(x,0) for x in l), 16)
print k