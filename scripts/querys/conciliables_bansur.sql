WITH BANSUR_CONCILIABLES AS (
    SELECT
        SUBSTRING(TARJETA FROM 1 FOR 6) AS PRIMEROS_6_DIGITOS,
        SUBSTRING(TARJETA FROM 6 for 4) AS ULTIMOS_4_DIGITOS,
        ID_ADQUIRIENTE AS ID,
        MONTO,
        FECHA_TRANSACCION,
        ROW_NUMBER() OVER (PARTITION BY ID_ADQUIRIENTE ORDER BY FECHA_TRANSACCION DESC) AS RN
    FROM bansur
    WHERE TIPO_TRX = 'PAGO'
)
SELECT
    ID,
    SUM(MONTO) AS MONTO_TOTAL,
    COUNT(*) AS CANTIDAD_TRANSACCIONES
FROM
    BANSUR_CONCILIABLES
WHERE
    RN = 1
GROUP BY
    ID;