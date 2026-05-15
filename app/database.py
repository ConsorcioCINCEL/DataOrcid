"""Database seed data for initial admin and institution accounts."""

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

        institutions = [
            ("ANID", "Agencia Nacional de Investigación y Desarrollo", "02ap3w078"),
            ("PUC", "Pontificia Universidad Católica de Chile", "04teye511"),
            ("UADV", "Universidad Adventista", "038j0b276"),
            ("UNAB", "Universidad Nacional Andrés Bello", "01qq57711"),
            ("UNAP", "Universidad Arturo Prat", "01hrxxx24"),
            ("UCSC", "Universidad Católica de la Santísima Concepción", "03y6k2j68"),
            ("UCT", "Universidad Católica de Temuco", "051nvp675"),
            ("UCN", "Universidad Católica del Norte", "02akpm128"),
            ("UA", "Universidad de Antofagasta", "04eyc6d95"),
            ("UChile", "Universidad de Chile", "047gc3g35"),
            ("UdeC", "Universidad de Concepción", "0460jpj73"),
            ("ULA", "Universidad de La Serena", "01ht74751"),
            ("UAndes", "Universidad de Los Andes", "03v0qd864"),
            ("ULagos", "Universidad de Los Lagos", "05jk8e518"),
            ("USACH", "Universidad de Santiago de Chile", "02ma57s91"),
            ("UTA", "Universidad de Tarapacá", "04xe01d27"),
            ("UV", "Universidad de Valparaíso", "00h9jrb69"),
            ("UDD", "Universidad del Desarrollo", "05y33vv83"),
            ("UTFSM", "Universidad Técnica Federico Santa María", "05510vn56"),
            ("UO", "Universidad de O'Higgins", "044cse639"),
            ("UMag", "Universidad de Magallanes", "049784n50"),
            ("UFinis", "Universidad Finis Terrae", "0225snd59"),
            ("UACH", "Universidad Austral de Chile", "029ycp228"),
        ]

        for username, inst_name, ror_id in institutions:
            if not User.query.filter_by(username=username).first():
                temp_pwd = generate_temp_password(12)
                
                new_inst_user = User(
                    username=username, 
                    institution_name=inst_name, 
                    ror_id=ror_id
                )
                new_inst_user.set_password(temp_pwd)
                db.session.add(new_inst_user)
                created_count += 1
                
                logger.info("Institutional user '%s' created. Password: %s", username, temp_pwd)

        db.session.commit()
        logger.info("Database seeding completed. %d users created.", created_count)
        
        return "Seeding process completed successfully", 200

    except Exception as exc:
        db.session.rollback()
        logger.exception("CRITICAL: Database seeding failed: %s", exc)
        return f"Database error: {str(exc)}", 500
