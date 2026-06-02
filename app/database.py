"""Database seed data for the initial admin account."""

import logging
from typing import Tuple
from . import db
from .models import User
from .utils.passwords import generate_temp_password

logger = logging.getLogger(__name__)


def populate_users() -> Tuple[str, int]:
    """
    Seed missing default accounts without overwriting existing credentials.
    """
    try:
        created_count = 0

        admin_user = User.query.filter_by(username="admin").first()
        
        if not admin_user:
            admin_pwd = generate_temp_password(14)
            
            new_admin = User(
                username="admin",
                is_admin=True,
                institution_name="System Administrator",
                ror_id="02ap3w078",  # Default ROR (ANID) for system management context
            )
            new_admin.set_password(admin_pwd)
            db.session.add(new_admin)
            created_count += 1

            logger.info("System Admin created. Temporary password: %s", admin_pwd)

        db.session.commit()
        logger.info("Database seeding completed. %d users created.", created_count)
        
        return "Seeding process completed successfully", 200

    except Exception as exc:
        db.session.rollback()
        logger.exception("CRITICAL: Database seeding failed: %s", exc)
        return f"Database error: {str(exc)}", 500
