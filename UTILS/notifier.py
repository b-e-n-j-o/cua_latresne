# notifier.py
import os, smtplib
from pathlib import Path
from email.message import EmailMessage

# Jinja2 pour les templates
from jinja2 import Environment, FileSystemLoader, select_autoescape

TEMPLATES_DIR = Path(__file__).parent / "templates"
env = Environment(
    loader=FileSystemLoader(str(TEMPLATES_DIR)),
    autoescape=select_autoescape(["html", "xml"]),
)

def render_email(template_name: str, **ctx) -> str:
    """
    Rend un template HTML depuis templates/emails/<template_name>
    """
    tpl = env.get_template(f"emails/{template_name}")
    return tpl.render(**ctx)

# Resend (optionnel)
_HAS_RESEND = False
try:
    import resend  # pip install resend
    if os.getenv("RESEND_API_KEY"):
        resend.api_key = os.getenv("RESEND_API_KEY")
        _HAS_RESEND = True
except Exception:
    _HAS_RESEND = False

MAIL_FROM = os.getenv("MAIL_FROM", "no-reply@example.com")

def send_mail(to, subject: str, html: str, text: str | None = None) -> bool:
    """
    Envoie un mail via Resend si dispo, sinon SMTP si configuré.
    to: list[str] | tuple[str,...] | set[str,...]
    """
    if not to:
        return False
    recipients = [x for x in to if x]

    # 1) Resend
    if _HAS_RESEND:
        try:
            resend.Emails.send({
                "from": MAIL_FROM,
                "to": recipients,
                "subject": subject,
                "html": html or "",
                "text": text or "",
            })
            return True
        except Exception:
            pass

    # 2) SMTP
    host = os.getenv("SMTP_HOST")
    if host:
        port = int(os.getenv("SMTP_PORT", "587"))
        user = os.getenv("SMTP_USER") or None
        pwd  = os.getenv("SMTP_PASS") or None
        use_tls = os.getenv("SMTP_TLS", "1") != "0"

        msg = EmailMessage()
        msg["From"] = MAIL_FROM
        msg["To"] = ", ".join(recipients)
        msg["Subject"] = subject
        if text:
            msg.set_content(text)
        if html:
            msg.add_alternative(html, subtype="html")

        with smtplib.SMTP(host, port, timeout=20) as s:
            if use_tls:
                s.starttls()
            if user and pwd:
                s.login(user, pwd)
            s.send_message(msg)
        return True

    return False
