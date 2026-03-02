from blueshift.api import (
    symbol,
    order_target_percent,
)

def initialize(context):
    """
    Inicializa la estrategia: invierte el 100% en APP al comienzo.
    """
    # Selección del activo
    context.target_stock = symbol('APP')

    # Ejecuta la orden de compra al 100% al inicio
    order_target_percent(context.target_stock, 1.0)
