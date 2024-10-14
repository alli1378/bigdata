
from simple_websocket_server import WebSocketServer, WebSocket
import json
class SimpleEcho(WebSocket):
    def handle(self):
        print('fuck',self.data)
        if self.data is None:
            print('fuck')
        else:
            for client in self.server.connections.values():
                print('ssssss')
                client.send_message(json.dumps(self.data))
        #else:
        #    for client in self.server.connections.values():
        #        print('dd',self.data)
        #        client.send_message(json.dumps(self.data))
    def connected(self):
        print(self.address, 'connected')

    def handle_close(self):
        print(self.address, 'closed')


server = WebSocketServer('*', 4001, SimpleEcho)


server.serve_forever()
