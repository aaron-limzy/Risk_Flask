from app import create_app

from waitress import serve

server = create_app()

serve(server, host='0.0.0.0', port=5000)
#server.run()
#server.app_context().push()