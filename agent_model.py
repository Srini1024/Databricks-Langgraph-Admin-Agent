import uuid
import inspect
import json
import mlflow
import mlflow.pyfunc
from databricks_langchain import ChatDatabricks
from langchain_core.messages import SystemMessage, HumanMessage, AIMessage
from langchain_core.tools import BaseTool
from langchain_core.utils.function_calling import convert_to_openai_tool
from langgraph.graph import StateGraph, MessagesState, END, START
from langgraph.prebuilt import ToolNode, tools_condition
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
)
import tools 

SYSTEM_PROMPT = """You are a helpful assistant. You have access to tools to answer questions"""

def discover_tools():
    """
    Dynamically imports the 'tools' module and returns a list of 
    all LangChain tool objects defined within it.
    """
    
    def is_langchain_tool(obj):
        
        return isinstance(obj, BaseTool)

    tool_objects = [
        obj 
        for name, obj in inspect.getmembers(tools, is_langchain_tool)
    ]
    
    return tool_objects

class Graphbuilder():

    def __init__(self):

        """

        Constructor which will get initialized during start

        """

        print("Initializing Graphbuilder...")

        self.llm = ChatDatabricks(

            endpoint="databricks-gpt-oss-20b",

            temperature=0

        )

        print("ChatDatabricks LLM loaded.")


        self.tools= discover_tools()
        
        def fix_schema_recursive(obj):
            """Recursively remove additionalProperties from all nested objects"""
            if isinstance(obj, dict):
                if 'additionalProperties' in obj:
                    del obj['additionalProperties']
                for key, value in list(obj.items()):
                    fix_schema_recursive(value)
            elif isinstance(obj, list):
                for item in obj:
                    fix_schema_recursive(item)
            return obj
        
        openai_tools = []
        for tool in self.tools:
            tool_dict = convert_to_openai_tool(tool)
            fix_schema_recursive(tool_dict)
            
            openai_tools.append(tool_dict)
        
        self.llm_with_tools = self.llm.bind_tools(tools=openai_tools)

        self.system_prompt = SYSTEM_PROMPT

        self.graph = None



    def agent_function(self, state: MessagesState):

        """

        Main agent function

        """

        print("Running agent_function...")

        messages = state['messages']

        input_messages = [SystemMessage(content=self.system_prompt)] + messages

        response = self.llm_with_tools.invoke(input_messages)

        return {'messages': [response]}



    def build_graph(self):

        """

        Builds the LangGraph graph

        """

        print("Building graph...")

        graph_builder = StateGraph(MessagesState)

       

        graph_builder.add_node('agent', self.agent_function)

        graph_builder.add_node('tools', ToolNode(tools=self.tools))

       

        graph_builder.add_edge(START, 'agent')

        graph_builder.add_conditional_edges(

            'agent',

            tools_condition,

            {

                "tools": "tools",

                END: END

            }

        )

        graph_builder.add_edge('tools', 'agent')



        self.graph = graph_builder.compile()

        print("Graph built and compiled.")

        return self.graph


class DatabricksAgentWrapper(ResponsesAgent):

    def __init__(self):
        """Initialize the agent on creation."""
        super().__init__()
        self.graph_builder = None
        self.app = None
        self.load_context(None)

    def load_context(self, context):

        """

        Called once when the model is loaded in the serving environment.

        """

        print("DatabricksAgentWrapper: Loading context...")

        try:

            self.graph_builder = Graphbuilder()

            self.app = self.graph_builder.build_graph()

            print("DatabricksAgentWrapper: LangGraph agent built and compiled successfully.")

        except Exception as e:

            print(f"DatabricksAgentWrapper: ERROR building graph: {e}")

            raise e

   

    def create_text_output_item(self, text: str, id: str):

        """

        Creates a text output item in the correct format for ResponsesAgentResponse.

        """

        return {

            "type": "message",

            "id": id,

            "content": [{"type": "output_text", "text": text}],

            "role": "assistant",

        }



    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:

        """

        Called for every API request from the chat playground.

        """

        try:

            request_dict = request.model_dump()

        except AttributeError:

            try:

                request_dict = request.dict()

            except AttributeError:

                request_dict = request

       

        if "input" not in request_dict:

             print(f"ERROR: 'input' key not in request. Keys found: {request_dict.keys()}")

             return ResponsesAgentResponse(

                 id=str(uuid.uuid4()),

                 output=[

                     self.create_text_output_item(

                         text="Error: Model received invalid request format.",

                         id=str(uuid.uuid4())

                     )

                 ]

             )



        print(f"DatabricksAgentWrapper: Processing {len(request_dict['input'])} messages.")

        langgraph_messages = []

       

        for msg in request_dict["input"]:
            
            content = msg["content"]
            if isinstance(content, list):

                content = " ".join([item.get("text", "") for item in content if isinstance(item, dict) and "text" in item])
            
            if msg["role"] == "user":

                langgraph_messages.append(HumanMessage(content=content))

            elif msg["role"] == "assistant":

                langgraph_messages.append(AIMessage(content=content))

            elif msg["role"] == "system":

                langgraph_messages.append(SystemMessage(content=content))

       

        initial_state = {"messages": langgraph_messages}



        try:

            output_state = self.app.invoke(initial_state)

            final_response_message = output_state['messages'][-1]

            final_text = final_response_message.content

            return ResponsesAgentResponse(

                id=str(uuid.uuid4()),

                output=[

                    self.create_text_output_item(

                        text=final_text,

                        id=str(uuid.uuid4())

                    )

                ]

            )



        except Exception as e:

            print(f"DatabricksAgentWrapper: Error during agent invocation: {e}")

            return ResponsesAgentResponse(

                id=str(uuid.uuid4()),

                output=[

                    self.create_text_output_item(

                        text=f"Sorry, an error occurred: {str(e)}",

                        id=str(uuid.uuid4())

                    )

                ]

            )

AGENT = DatabricksAgentWrapper()
mlflow.models.set_model(AGENT)