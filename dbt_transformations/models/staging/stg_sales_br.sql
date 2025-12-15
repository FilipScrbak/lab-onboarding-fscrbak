with br_sales as (
select * from {{source("raw", "br_sales_src")}}
)

select
 Cabecalho.TransacaoID as transaction_id,
 Cabecalho.TransacaoTempo as transaction_time,
 Cabecalho.LojaID as store_id,
 Cabecalho.Cliente.ClienteID as customer_id,
 Modelo.ModeloID as model_id,
 null as employee_id,
 null as quantity,
 to_hex(sha256(cast(Cabecalho.Cartao.Numero as string))) as card_number_hash,
 Modelo.Preco as total_amount



    FROM br_sales
