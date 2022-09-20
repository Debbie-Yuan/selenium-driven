import smtplib


# Currently we cannot send emails directly from amazon ec2 servers.
# They banned outbound SMTP port 25.
account = "debbie@remote.hypertension.icu"
host = "remote.hypertension.icu"
to = ["debbie_yuan@icloud.com", "447569415@qq.com"]
smt = smtplib.SMTP(host=host)
smt.set_debuglevel(1)
smt.login(user=account, password="2364")
ret = smt.sendmail(account, to, "hello, debbie!")
